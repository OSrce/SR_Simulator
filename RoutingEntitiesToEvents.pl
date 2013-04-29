#!/usr/bin/perl

#TODO Make this update the event with whether or not the entity has responded. The event shouldn't end otherwise
#The event should also update itself with random updates

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
		   	 #print "$eventquery \n";
		   	                          
	$eventquery_handle = $srdb->prepare($eventquery);
	$eventquery_handle->execute();
	$eventquery_handle->bind_columns(undef, \$eventid);
	
	
	#loop through the events
	while($eventquery_handle->fetch()){
	
		#print "$eventid is happening right now \n";
	
		#If an event does not have an assigned entity then assign it one
		$assignedentityid="";
		
		$assignedquery = "select entity from entity_status es where es.data @> '\"assigned\"=>\"t\"' AND es.data @> '\"event_id\"=>\"$eventid\"' limit 1";		
		#print "finding assigned unit: $assignedquery \n";
								  
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
				 
			#find the closest gid to entity and the closest to the event -- use source (not sure if this is right, but using gid sometimes gave errors)
			$startgidquery = "select source, st_distance(geom, '$entitylocation') from tigerroads order by st_distance(geom, '$entitylocation')  limit 1";							  
			$startgidquery_handle = $adb->prepare($startgidquery);
			$startgidquery_handle->execute();
			$startgidquery_handle->bind_columns(undef, \$startgid, \$ignorethis);
			$startgidquery_handle->fetch();
	
			#find the closest gid to entity and the closest to the event -- use target (not sure if this is right)
			$endgidquery = "select target, st_distance(geom, '$eventlocation') from tigerroads order by st_distance(geom, '$eventlocation')  limit 1";		
			$endgidquery_handle = $adb->prepare($endgidquery);
			$endgidquery_handle->execute();
			$endgidquery_handle->bind_columns(undef, \$endgid, \$ignorethis);
			$endgidquery_handle->fetch();
	
			#Route the entity to the event and stick that route in srmap
			$routetoevent="";
			
			$routequery = "SELECT st_force_3d(st_makeline(the_geom)) FROM calc_route('tigerroads', $startgid, $endgid) AS (start_id int, end_id int, id int, gid int, the_geom geometry)";
			$routequery_handle = $adb->prepare($routequery);
			$routequery_handle->execute();
			$routequery_handle->bind_columns(undef, \$routetoevent);
			$routequery_handle->fetch();
			
			if($routetoevent != ""){		
				print "			Assigning $entityid \n";
				#Close the old status where assigned=f
				my $rows = $srdb->do("update  entity_status set data_end=now() where data @> '\"assigned\"=>\"f\"' and entity=$entityid and has_end='f'");
				#assign the entity to that event
				my $rows = $srdb->do("insert into entity_status (entity, data, data_begin) values ($entityid, hstore(ARRAY[['event_id','$eventid'], ['assigned','t'], ['enroute','f'], ['onscene','f']]), now() )");
	
				my $rows = $srdb->do("insert into srmap (group_id, geometry, data) values(2015, '$routetoevent', hstore(ARRAY[['entity','$entityid'], ['event','$eventid']])) \n");
				print "$entityid is going to event $eventid and it worked ($rows)\n";
			} else {
				print "			###Null routequery: $routequery \n";
				
					#Pretend this was just a false alarm, 
					my $rows = $srdb->do("UPDATE event set data = data || hstore('cfs_body', data->'cfs_body' || 'FALSE ALARM  . . .') where id=$eventid AND has_end='f'");
					#End the event
					my $rows = $srdb->do("UPDATE event set data = data || '\"cfs_finaldis\"=>\"00\"'::hstore, data_end=now() where id=$eventid");
					
			}

		} else {
			$entityid="";
			$onscene=="";
			$enroute=="";
			
			#Find out if the unit is enroute or on scene or not
			$onscenequery = "select entity, data->'onscene' as onscene, data->'enroute' as enroute from entity_status where data @> '\"event_id\"=>\"$eventid\"' AND (data @> '\"onscene\"=>\"t\"' OR data @> '\"onscene\"=>\"f\"')  AND (data @> '\"enroute\"=>\"t\"' OR data @> '\"enroute\"=>\"f\"') AND has_end='f' limit 1";											  
			$onscenequery_handle = $srdb->prepare($onscenequery);
			$onscenequery_handle->execute();
			$onscenequery_handle->bind_columns(undef, \$entityid, \$onscene, \$enroute);
			$onscenequery_handle->fetch();
			
			#If the result is not NULL
			if($entityid!=""){
				
				if($onscene eq "t"){
					print "Unit $entityid on scene \n";
					#Delete the route from the map
					my $rows = $srdb->do("DELETE FROM srmap where data @> '\"entity\"=>\"$entityid\"' AND group_id=2015" );
					#Update the event: unit on scene:
					my $rows = $srdb->do("UPDATE event set data = data || hstore('cfs_body', data->'cfs_body' || 'Unit on scene . . .') where id=$eventid AND has_end='f' ");
					
					
					#If the unit is on scene then wait some random amount of time (or if (rand()>0.9) ...) to set the final disposition
					#add final disposition, time etc.	
					if(rand()>0){
						my $finaldis=int(rand(8)) +91;				
						my $rows = $srdb->do("UPDATE event set data = data || '\"cfs_finaldis\"=>\"$finaldis\"'::hstore, data_end=now() where id=$eventid");
						
						#Once there is a finaldis unassign the entity and take it off the map
						my $rows = $srdb->do("update entity_status set data_end=now() where data @> '\"event_id\"=>\"$eventid\"' AND data @> '\"onscene\"=>\"t\"' AND has_end='f'");
					
						#Put it back into the unassigned category
						my $rows = $srdb->do("insert into entity_status (entity, data, data_begin) values ($entityid, hstore(ARRAY[['inservice','t'],['assigned','f'], ['onscene','f'], ['enroute','f']]), now() )");
					
						#Update the location status - Insert a new one with the same location
						my $rows = $srdb->do("insert into entity_status (entity, data) values ($entityid, hstore(ARRAY[['heading','0'],['assigned','f'], ['routeid','']]))");
						#Set the old one to end
						my $rows = $srdb->do("update entity_status set data_end=now() where entity='$entityid' and has_end='f' and data @> '\"assigned\"=>\"t\"' ");
										
					
						#This is dealt with by a symbolizer rule instead now
						#my $rows = $srdb->do("update entity set group_id=0 where id=$entityid");
					}				
				}
				
				if($enroute eq "t"){
					#Update the event: unit on scene:
					my $rows = $srdb->do("UPDATE event set data = data || hstore('cfs_body', data->'cfs_body' || 'Unit en route  . . .') where id=$eventid AND has_end='f' AND data->'cfs_body' !~'en route' ");
					if($rows>0){
						print "$entityid is en route\n";
					}
				}					

						
			}
			


			
		}
		
		sleep($timeinterval);
		
		
	}
}




