#!/usr/bin/perl

# load module
use DBI;
use POSIX qw/strftime/;
use Date::Calc qw(Add_Delta_DHMS);

my $timeinterval = 1;

# connect to database
my $srdb = DBI->connect("DBI:Pg:dbname=sitrepdev;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});
my $adb = DBI->connect("DBI:Pg:dbname=alexdb;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});


my $rows = $srdb->do("DELETE FROM srmap where data ? 'entity' AND group_id=2015" );
  

while(1){


	#Find all the events ...
		#Select all the route
	$eventquery = "SELECT id FROM event where has_end='f'";
		   	 print "$eventquery \n";
		   	                          
	$eventquery_handle = $srdb->prepare($eventquery);
	$eventquery_handle->execute();
	$eventquery_handle->bind_columns(undef, \$eventid);
	
	
	#loop through the events
	while($eventquery_handle->fetch()){
	
		print "$eventid is happening right now \n";
	
		#If an event does not have an assigned entity then assign it one
		$assignedentityid="";
		
		$assignedquery = "select entity from entity_status es where es.data @> '\"assigned\"=>\"t\"' AND es.data @> '\"event_id\"=>\"$eventid\"' limit 1";		
		print "finding assigned unit: $assignedquery \n";
								  
		$assignedquery_handle = $srdb->prepare($assignedquery);
		$assignedquery_handle->execute();
		$assignedquery_handle->bind_columns(undef, \$assignedentityid, );
		$assignedquery_handle->fetch();		
		
		
		if($assignedentityid==""){
			#find the closest free entity

			$entityquery = "select es.entity, st_distance(l.geometry, l2.geometry), l.geometry, l2.geometry from event ev, entity_status es, (select geometry from location, event where event.id=$eventid AND event.location=location.id) l2, (select geometry, entity from location, entity_status es where es.location=location.id) l  where ev.id=$eventid AND es.entity=l.entity AND es.has_end='f' AND es.data @> '\"assigned\"=>\"f\"' AND es.data @> '\"inservice\"=>\"t\"' order by st_distance(l.geometry, l2.geometry) limit 1";								  
			$entityquery_handle = $srdb->prepare($entityquery);
			$entityquery_handle->execute();
			$entityquery_handle->bind_columns(undef, \$entityid, \$distancetoevent, \$entitylocation, \$eventlocation);
			$entityquery_handle->fetch();		
			
			print "$entityquery \n";
			print "insert into entity_status (entity, data, data_begin) values ($entityid, hstore(ARRAY[['event_id','$eventid'], ['assigned','t'], ['enroute','f'], ['onscene','f']]), now() ) \n";
			
			
			#assign the entity to that event
			my $rows = $srdb->do("insert into entity_status (entity, data, data_begin) values ($entityid, hstore(ARRAY[['event_id','$eventid'], ['assigned','t'], ['enroute','f'], ['onscene','f']]), now() )");
	
	 
			#find the closest gid to entity and the closest to the event -- use source (not sure if this is right, but using gid sometimes gave errors)
			$startgidquery = "select source, st_distance(geom, '$entitylocation') from tigerroads order by st_distance(geom, '$entitylocation')  limit 1";	
			
			print "$startgidquery \n";
										  
			$startgidquery_handle = $adb->prepare($startgidquery);
			$startgidquery_handle->execute();
			$startgidquery_handle->bind_columns(undef, \$startgid, \$ignorethis);
			$startgidquery_handle->fetch();
	
			#find the closest gid to entity and the closest to the event -- use target (not sure if this is right)
			$endgidquery = "select target, st_distance(geom, '$eventlocation') from tigerroads order by st_distance(geom, '$eventlocation')  limit 1";		
			print "$endgidquery \n";
			$endgidquery_handle = $adb->prepare($endgidquery);
			$endgidquery_handle->execute();
			$endgidquery_handle->bind_columns(undef, \$endgid, \$ignorethis);
			$endgidquery_handle->fetch();
	
	
			#Route the entity to the event and stick that route in srmap
		
			
			#$routequery = "SELECT st_force_3d(st_makeline(tigerroads.geom)) FROM (select * from shortest_path('SELECT gid as id, source::integer,target::integer,length as cost FROM tigerroads', $startgid, $endgid, false, false)) a, tigerroads where a.edge_id= tigerroads.gid";								  
			$routequery = "SELECT st_force_3d(st_makeline(the_geom)) FROM calc_route('tigerroads', $startgid, $endgid) AS (start_id int, end_id int, id int, gid int, the_geom geometry)";
			print "$routequery \n";
		
			$routequery_handle = $adb->prepare($routequery);
			$routequery_handle->execute();
			$routequery_handle->bind_columns(undef, \$routetoevent);
			$routequery_handle->fetch();
		
			
			print "The route is $routetoevent \n";
			
			
			my $rows = $srdb->do("insert into srmap (group_id, geometry, data) values(2015, '$routetoevent', hstore(ARRAY[['entity','$entityid']]))");
		} else {
			$entityid="";
			$onscenequery = "select entity from entity_status where data @> '\"event_id\"=>\"$eventid\"' AND data @> '\"onscene\"=>\"t\"' AND has_end='f' limit 1";	
			
			print "$onscenequery \n";
										  
			$onscenequery_handle = $adb->prepare($onscenequery);
			$onscenequery_handle->execute();
			$onscenequery_handle->bind_columns(undef, \$entityid);
			$onscenequery_handle->fetch();
			
			if($entityid!=""){
				my $rows = $srdb->do("DELETE FROM srmap where data @> '\"entity\"=>\"$entityid\"' AND group_id=2015" );
			}
			
		}
		
		sleep($timeinterval);
		
		
	}
}


sub checkForNewEvents {
	my ( $self ) = @_;
	my @tmp = $self->generateUpdates();
	$self->{updatenum}++;
	if($self->{updatenum} - 1  >= @tmp){
		my $tmpnum = $self->{updatenum} -1;
		return "Update $tmpnum . . .";
	} else {
		return $tmp[$self->{updatenum}-1];
	}

}

sub findClosestAvailableUnit {
	my($eventid) = @_;
	
	
	#Select all events
	$select = "select es.entity, st_distance(l.geometry, l2.geometry) from event ev, entity_status es, location l, (select geometry from location, event where event.id='$eventid' AND event.location=location.id) l2  where ev.id='$eventid' AND es.location=l.id AND es.has_end='f' AND es.data @> '\"assigned\"=>\"f\"' AND es.data @> '\"inservice\"=>\"t\"' order by st_distance(l.geometry, l2.geometry) limit 1";
	print "$select \n";
	$select_handle = $srdb->prepare($select);
	$select_handle->execute();
	$select_handle->bind_columns(undef, \$unitid, \$distanceaway);
	$select_handle->fetch();
		print "Only $distanceaway degrees away\n";
	return $unitid;
	
}

sub getRoute {
	my ( $self ) = @_;
	my @tmp = $self->generateUpdates();
	$self->{updatenum}++;
	if($self->{updatenum} - 1  >= @tmp){
		my $tmpnum = $self->{updatenum} -1;
		return "Update $tmpnum . . .";
	} else {
		return $tmp[$self->{updatenum}-1];
	}

}


