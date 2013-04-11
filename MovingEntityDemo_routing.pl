#!/usr/bin/perl

# load module
use DBI;
use POSIX qw/strftime/;
use Date::Calc qw(Add_Delta_DHMS);

my $timeinterval = 5;

# connect to database
my $srdb = DBI->connect("DBI:Pg:dbname=sitrepdev;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});

my $rows = $srdb->do("DELETE FROM entity_status where id in (SELECT es.id as id from entity_status es, entity e where es.entity=e.id AND (e.group_id=2014 OR e.group_id=0))" );


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
	

	$i=0;
	while($query_handle->fetch()) {
		
		push @entity_list, $entityid;
		#random direction and speed
		my $speed = (100 - int(rand(50)))/5000;
		#if(rand()>0.5) {
		#	$speed = $speed *-1;
		#}
		
		#Is there a reason to have "speed" even when it's not moving?? I have to initialize that somewhere I guess.
		push @velocity_list, $speed;
		push @route_list, "";
		
		#initialize with starting point
		#push @pct_along_route_list, rand();
		push @pct_along_route_list, 0;
		
		#place every entity on the map
		#insert initial location for each entity at the fire station... f
		$insertlocations = "insert into location (source, data ,geometry) select 6, hstore(ARRAY[['type','fire truck']]), geometry from srmap, entity where srmap.id=entity.name::int AND entity.id=$entityid returning id, st_x(geometry), st_y(geometry)  ";
		
		##$insertlocations = "insert into location (source, data ,geometry) select 6, hstore(ARRAY[['type','fire truck']]), st_line_interpolate_point(geometry, $pct_along_route_list[$i]) from srmap where id=$route_list[$i] returning id, st_x(geometry), st_y(geometry)  ";
		
		
	#	print "$insertlocations\n";
		$insertlocations_handle = $srdb->prepare($insertlocations);
		$insertlocations_handle->execute();
		#get the returned id
		$insertlocations_handle->bind_columns(undef, \$locid, \$x, \$y);
		#$locid = $insertlocations_handle->fetch()->[0];
		$insertlocations_handle->fetch();
		push @x_list, $x;
		push @y_list, $y;
		
		
		#print "Entity $entityid moving $speed along $routeid at $x and $y\n";
		$heading = 0;
		#update entity status
	
		#Insert a baseline status 
		my $rows = $srdb->do("insert into entity_status (entity, data, data_begin) values ($entity_list[$i], hstore(ARRAY[['inservice','t'],['assigned','f'],['event_id',''], ['onscene','f'], ['enroute','f']]), now() )");
	
		$insertstatus = "insert into entity_status (entity, location, data, data_begin) values ($entity_list[$i], $locid, hstore(ARRAY[['heading','$heading'], ['routeid','']]), now() ) returning id, entity";
	#	print "insertstatus is $insertstatus\n";

		$insertstatus_handle = $srdb->prepare($insertstatus);
		$insertstatus_handle->execute();
		#get the returned status id
		$insertstatus_handle->bind_columns(undef, \$statusid, \$updatedentityid);
		$insertstatus_handle->fetch();

		#$statusid = $insertstatus_handle->fetch()->[0];
		
		print "Status # $i $statusid for truck # $updatedentityid\n";
		
		
		$i++;
	}
	
	#MOVE FIRE TRUCKS TO A DIFFERENT LAYER WHEN THEY ARE ROUTING
	

	$num_entities = @entity_list;
	print "There are $num_entities fire trucks \n";
	
	
	#update location
	while(1){
	
		sleep(1);
		
		#Find the route to the event and assign it to the unit
		
		#Update the positions of the units
		
		$num_entities = @entity_list;

		$i=0;
		for ($i = 0; $i < $num_entities; $i++) {
			
			$routeid = "";
			
			#Every entity should have a route in SRmap ... or if there is no route, then that means it should stay where it is.
			$routequery = "select id from srmap where group_id=2015 AND data @> '\"entity\"=>\"$entity_list[$i]\"' limit 1";
			$routequery_handle = $srdb->prepare($routequery);
			$routequery_handle->execute();
			$routequery_handle->bind_columns(undef, \$routeid);
			$routequery_handle->fetch();

			if( ($routeid != "") && ( $route_list[$i]!= $routeid) ){
			
				print "Found new route for $entity_list[$i] along route $routeid \n";
				$route_list[$i] = $routeid;
				

				#Once it's routed then 				
				my $rows = $srdb->do("UPDATE entity set group_id=2014 where id=$entity_list[$i]" );
					#maybe if the route changes it should restart pct_along_route_list[$i] to 0
			}		
			
			
			#If there is a route, then calculate the pct along the route
			if($route_list[$i]!=""){
				
				if($pct_along_route_list[$i] !=-1){
					$pct_along_route_list[$i] = $pct_along_route_list[$i] + $velocity_list[$i];
				}
				
				#if they reach the end of their route, then set onscene=true
				if($pct_along_route_list[$i] >= 1){
					
					print "#############$entity_list[$i] has reached it's destination!! Hallelujah! \n";
					
					$pct_along_route_list[$i] = 1;
					#update the table so that "onscene" is true
					my $rows = $srdb->do("insert into entity_status (entity, location, data, data_begin) values ($entity_list[$i], $locid, hstore(ARRAY[['heading','$heading'], ['routeid','$routeid']]), now() ) returning id");
					
					#update the old status
					#my $rows = $srdb->do("update entity_status set data_end=now() where data @> '\"event_id\"=>\"$eventid\"' AND data @> '\"onscene\"=>\"f\"' AND entity=$entity_list[$i] AND has_end='f'");
					#Shouldn't event id be set somewhere in this loop?
				
				#	my $rows = $srdb->do("update entity_status set data_end=now() where data @> '\"onscene\"=>\"f\"' AND entity=$entity_list[$i] AND has_end='f' returning data -> 'event_id'");
					
					$eventquery = "update entity_status set data_end=now() where data @> '\"onscene\"=>\"f\"' AND entity=$entity_list[$i] AND has_end='f' returning data -> 'event_id'";
					$eventquery_handle = $srdb->prepare($eventquery);
					$eventquery_handle->execute();
					$eventquery_handle->bind_columns(undef, \$eventid);
					$eventquery_handle->fetch();
					
					print "    						Entity $entity_list[$i] is on scene at $eventid \n";
				
					
					#Shouldn't event id be set somewhere in this loop?
					#insert new status "on scene"
					my $rows = $srdb->do("insert into entity_status (entity, data, data_begin) values ($entity_list[$i], hstore(ARRAY[['event_id','$eventid'], ['onscene','t'], ['enroute','f']]), now() )");
					
					#take the entity off the mape, and unassign it the event (in reality it should route back to the station
					#this should probably include the entityid not the eventid or maybe include both
					my $rows = $srdb->do("update entity_status set data_end=now() where data @> '\"event_id\"=>\"$eventid\"' AND data @> '\"onscene\"=>\"t\"' AND has_end='f'");
					
					my $rows = $srdb->do("update entity set group_id=0 where id=$entity_list[$i]");
					
					
					#TODO Delete the route so that entity position stops being updated. Should I do that here or in RoutingEntitiesToEvents.pl??
					
					#$velocity_list[$i] = $velocity_list[$i]*-1;
					#$pct_along_route_list[$i] = $pct_along_route_list[$i] + 2*$velocity_list[$i];	
				
				}

				#On their way back???
				#if(($pct_along_route_list[$i] < 0)){
					#Use the same route, but in reverse??	
		
				#}
				
				
				if($pct_along_route_list[$i] != -1){
									
					#Create new location
					$insertlocations = "insert into location (source, data ,geometry) select 6, hstore(ARRAY[['type','fire truck']]), st_line_interpolate_point(geometry, $pct_along_route_list[$i]) from srmap where id=$route_list[$i] returning id, st_x(geometry), st_y(geometry)";
					print "$insertlocations\n";
				
					$insertlocations_handle = $srdb->prepare($insertlocations);
					$insertlocations_handle->execute();
					#get the returned id
					$insertlocations_handle->bind_columns(undef, \$locid, \$x, \$y);
					$insertlocations_handle->fetch();
	
					#After the final location has been updated, then set the pct to -1, just so it does not keep updating (as per if statement about 40 lines up)
					if($pct_along_route_list[$i] == 1){
						$pct_along_route_list[$i]=-1;
					}
					
					#To get this facing north I need to invert it and rotate it by 90.
					$heading = 90 + (-atan2($y - $y_list[$i], $x - $x_list[$i]) * 360/(2*3.14159));
					if($heading<0){
						$heading = $heading +360;
					}
					
					
					#If the position has changed at all then insert a new location
					if( ( $x_list[$i]!=$x) | ($y_list[$i]!=$y) ) {
					
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
			}
			
		}
	}
