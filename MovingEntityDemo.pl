#!/usr/bin/perl

# load module
use DBI;
use POSIX qw/strftime/;
use Date::Calc qw(Add_Delta_DHMS);

my $timeinterval = 5;

# connect to database
my $srdb = DBI->connect("DBI:Pg:dbname=sitrepdev;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});

my $rows = $srdb->do("DELETE FROM entity_status where id in (SELECT es.id as id from entity_status es, entity e where es.entity=e.id AND e.group_id=2014)" );

	#sleep($timeinterval);


	#Find a train for each trip
	#SELECT * FROM entity WHERE data @> '"type"=>"train"'::hstore;


	#Select all the trains that are active at this time (i.e. between two stops)
	#find the stations that each subway is between
	# PREPARE THE QUERY

	@entity_list;
	@route_list;
	@velocity_list;
	@pct_along_route_list;
	@x_list;
	@y_list;
	
	#Select all the entities
	$query = "select id from entity where data @> '\"type\"=>\"firetruck\"' OR data @> '\"type\"=>\"fireengine\"' OR data @> '\"type\"=>\"ambulance\"'";
	$query_handle = $srdb->prepare($query);
	$query_handle->execute();
	$query_handle->bind_columns(undef, \$entityid);
	
	$routequery = "select id from srmap where group_id=2015 order by random()";
	$routequery_handle = $srdb->prepare($routequery);
	$routequery_handle->execute();
	$routequery_handle->bind_columns(undef, \$routeid);
	
	$i=0;
	while($query_handle->fetch()) {
		
		push @entity_list, $entityid;
		#random direction and speed
		my $speed = (100 - int(rand(50)))/15000;
		if(rand()>0.5) {
			$speed = $speed *-1;
		}
		push @velocity_list, $speed;
		
		$routequery_handle->fetch();
		push @route_list, $routeid;
		
		
		
		#initialize with random location
		push @pct_along_route_list, rand();

		#place every entity on the map
		#insert initial location for each entity .... interpolate... f
		$insertlocations = "insert into location (source, data ,geometry) select 6, hstore(ARRAY[['type','fire truck']]), st_line_interpolate_point(geometry, $pct_along_route_list[$i]) from srmap where id=$route_list[$i] returning id, st_x(geometry), st_y(geometry)  ";
		print "$insertlocations\n";
		$insertlocations_handle = $srdb->prepare($insertlocations);
		$insertlocations_handle->execute();
		#get the returned id
		$insertlocations_handle->bind_columns(undef, \$locid, \$x, \$y);
		#$locid = $insertlocations_handle->fetch()->[0];
		$insertlocations_handle->fetch();
		push @x_list, $x;
		push @y_list, $y;
		
		
		print "Entity $entityid moving $speed along $routeid at $x and $y\n";
		$heading = 0;
		#update entity status
	
		$insertstatus = "insert into entity_status (entity, location, data, data_begin) values ($entity_list[$i], $locid, hstore(ARRAY[['heading','$heading'], ['routeid','$routeid']]), now() ) returning id";
		print "insertstatus is $insertstatus\n";

		$insertstatus_handle = $srdb->prepare($insertstatus);
		$insertstatus_handle->execute();
		#get the returned status id
		$statusid = $insertstatus_handle->fetch()->[0];
		
		
		
		
		$i++;
	}
	

	
	#update location
	while(1){

	print "#########\n";
	print "		x = @x_list ; y = @y_list \n";
	print "		@entity_list \n";
	print "		@velocity_list \n";
	print "		@route_list \n";
	print "		@pct_along_route_list \n";
	
		sleep(1);
		
		$num_entities = @entity_list;

		$i=0;
		for ($i = 0; $i < $num_entities; $i++) {
			
		
			$pct_along_route_list[$i] = $pct_along_route_list[$i] + $velocity_list[$i];
			
			#if they reach the end of their route, then turn them around by multipling speed*-1
			if(($pct_along_route_list[$i] > 1) || ($pct_along_route_list[$i] < 0)){
				$velocity_list[$i] = $velocity_list[$i]*-1;
				$pct_along_route_list[$i] = $pct_along_route_list[$i] + 2*$velocity_list[$i];	
			}
		

			
			#Create new location
			$insertlocations = "insert into location (source, data ,geometry) select 6, hstore(ARRAY[['type','fire truck']]), st_line_interpolate_point(geometry, $pct_along_route_list[$i]) from srmap where id=$route_list[$i] returning id, st_x(geometry), st_y(geometry)";
			#print "$insertlocations\n";
		
			$insertlocations_handle = $srdb->prepare($insertlocations);
			$insertlocations_handle->execute();
			#get the returned id
			$insertlocations_handle->bind_columns(undef, \$locid, \$x, \$y);
			
			$insertlocations_handle->fetch();
			
			#To get this facing north I need to invert it and rotate it by 90.
			$heading = 90 + (-atan2($y - $y_list[$i], $x - $x_list[$i]) * 360/(2*3.14159));
			if($heading<0){
				$heading = $heading +360;
			}
			
			if($i==0){
				#print "Entity $entity_list[$i] is $i and $heading. arctan(($y - $y_list[$i] ) / ($x - $x_list[$i] )) in degrees \n";
				print "Entity $entity_list[$i] is $i and New = $y  and Old = $y_list[$i] \n";
			}
			if($i==1){
				print "							Entity $entity_list[$i] is $i and New = $y  and Old = $y_list[$i] \n";
				#print "					Entity $entity_list[$i] is $i and $heading. arctan(($y - $y_list[$i] ) / ($x - $x_list[$i] )) in degrees \n";
			}
			$x_list[$i]=$x;
			$y_list[$i]=$y;
			
			#$locid = $insertlocations_handle->fetch()->[0];
			

			#print "Updating entity $entity_list[$i] \n";
			
			#update the old status
			my $rows = $srdb->do("UPDATE entity_status set data_end=now() where entity=$entity_list[$i] and has_end='f'" );
			
			#insert new status
			$insertstatus = "insert into entity_status (entity, location, data, data_begin) values ($entity_list[$i], $locid, hstore(ARRAY[['heading','$heading'], ['routeid','$routeid']]), now() ) returning id";
			$insertstatus_handle = $srdb->prepare($insertstatus);
			$insertstatus_handle->execute();
			#get the returned status id
			$statusid = $insertstatus_handle->fetch()->[0];
			#keep updating their locations
		
			
		}
	}


