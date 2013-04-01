#!/usr/bin/perl

# load module
use DBI;
use Job;
use POSIX qw/strftime/;



# connect to database
my $dbh = DBI->connect("DBI:Pg:dbname=sitrepdev;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});
#my $dbh = DBI->connect("DBI:Pg:dbname=sr_data;host=localhost", "alexkorb", "secret", {'RaiseError' => 1});

#start with a random CFS
@jobarray=("");
my $precinct=selectPrecinct();
$jobarray[0] = new Job(getHighestID()+1, $precinct);
$idnum = $jobarray[0]->getID;

#update cfs_body in database
my $update = $jobarray[0]->getUpdate();
my $date=strftime('%Y-%m-%d',localtime); 
my $code=int(rand(8)+31);
my $startime=strftime('%H:%M:%S', localtime);
my $address = $jobarray[@jobIDs]->getAddress();

my @cfsTypeArr = ("Fire","Medical", "Fire");
	

$cfspoint=createRandomLocation($precinct, $dbh);

#insert some location
$insertlocations = "insert into location (source, data ,geometry) values(6, hstore(ARRAY[['type','CFSDemo']]), St_Force_3D(St_GeomFromText( '$cfspoint', 4326) )   ) returning id";
print "$insertlocations\n";
$insertlocations_handle = $dbh->prepare($insertlocations);
$insertlocations_handle->execute();
#get the returned id
$locationid = $insertlocations_handle->fetch()->[0];

my $newcfstype = $cfsTypeArr[ int(rand(2) )] ;
my $rows = $dbh->do("INSERT INTO event (group_id, location, data) VALUES (999, $locationid, hstore(ARRAY[['cfs_type','$newcfstype'],['cfs_letter','K'], ['cfs_num','$idnum'], ['cfs_pct','$precinct'], ['cfs_sector','B'], ['cfs_code','$code'], ['cfs_addr','100 Test Street'], ['cfs_body','$update'], ['cfs_finaldis','']]))");


#for pseudorandomness to make sure not too many jobs are created in a row
my $jobsinarow=1;

@jobIDs=("$idnum"); #list of job IDs

my $joblimit=20; #limit the total number of jobs in the simulation
my $MaxTimeTilNextUpdate=50; #each job will be updated between 1 and 1 + MaxTimeTilNextUpdate seconds
my $updatechance=0.85; #likelihood of updating an existing CFS vs starting a new CFS
my $numjobs=1;

my @updatecounts = (0) x $joblimit; #The number of times a specific job has been updated; it's length is the number of possible jobs
my @jobupdatewaits=(0) x $joblimit; #The amount of time to wait before next updating each job
$jobupdatewaits[0]=3;

#This iterates through either updating or starting a new job, and waits 1 sec each time.
#for($iterations = 0; $iterations<30; $iterations++ ){
while(1){
	sleep(1);
	$numjobs=@jobIDs;
	#either update or choose a new event
	if(rand()<$updatechance || $jobsinarow>=2){
		#loop through all the jobs and see if they should be updated
		for ($i = 0; $i < $numjobs; $i++) {
			$jobupdatewaits[$i]--;
 			if($jobupdatewaits[$i]<1){
				$jobsinarow=0;
				#increase the counter for number of updates on a particular job
				$updatecounts[$i]++;
				my $tmpidnum = $jobarray[$i]->getID;
				my $tmpupdate = $jobarray[$i]->getUpdate();
				print "Update $updatecounts[$i] for job $jobIDs[$i]: $tmpupdate\n";
				
			#	my $rows = $dbh->do("UPDATE sr_cfs set cfs_body = cfs_body || '$tmpupdate' where cfs_num=$tmpidnum");
				my $rows = $dbh->do("UPDATE event set data = data || hstore('cfs_body', data->'cfs_body' || '$tmpupdate') where data @> '\"cfs_num\"=>\"$tmpidnum\"'");
				
				#EXAMPLE: update event set data=data || hstore('CustomersAffected', data->'CustomersAffected' || 'MoreData') WHERE group_id=1;
				
				#determine a time when this job will be updated next
				
				
				$jobupdatewaits[$i] = int(rand($MaxTimeTilNextUpdate)) + 1;

				#maybe change this to asking the job if it's done
				if($updatecounts[$i]>6){
					#then close the job	
					print "Job #$tmpidnum is closing.\n";
					#add final disposition, time etc.	
					my $finaldis=int(rand(8)) +91;
					my $finaldisdate=strftime('%Y-%m-%d %H:%M:%S', localtime);
					
					#my $rows = $dbh->do("UPDATE sr_cfs set cfs_finaldis = '$finaldis', cfs_finaldisdate = '$finaldisdate' where cfs_num=$tmpidnum");
					my $rows = $dbh->do("UPDATE event set data_end=now(), data = data || '\"cfs_finaldis\"=>\"$finaldis\"'::hstore where data @> '\"cfs_num\"=>\"$tmpidnum\"'");

					
					#replace the job with a new job
					$precinct=selectPrecinct();
		
					$code=int(rand(8)+31);
					$idnum++;
					$jobarray[$i] = new Job($idnum, $precinct);
					$jobupdatewaits[$i] = int(rand($MaxTimeTilNextUpdate)) + 1;
					$updatecounts[$i]=0;
					$update = $jobarray[$i]->getUpdate();
					$address = $jobarray[$i]->getAddress();
					
				
					my $startime=strftime('%H:%M:%S', localtime);

					#my $rows = $dbh->do("INSERT INTO sr_cfs (cfs_date, cfs_letter, cfs_num, cfs_pct, cfs_sector, cfs_code, cfs_addr, cfs_body, cfs_timecreated) 
					#		VALUES ('$date', 'K', '$idnum', '$precinct', 'B', $code, '$address', '$update', '$startime')");

					#Create random location
					$cfspoint=createRandomLocation($precinct, $dbh);
					
					#insert some location
					$insertlocations2 = "insert into location (source, has_data, data ,geometry) values(6, 't', hstore(ARRAY[['type','CFSDemo']]), St_Force_3D(St_GeomFromText( '$cfspoint', 4326) )   ) returning id";
					#print "$insertlocations2\n";
					$insertlocations2_handle = $dbh->prepare($insertlocations2);
					$insertlocations2_handle->execute();
					#get the returned id
					$locationid2 = $insertlocations2_handle->fetch()->[0];



					my $rows = $dbh->do("INSERT INTO event (group_id, location, data) VALUES (999, $locationid2, hstore(ARRAY[['cfs_type','Fire'],['cfs_letter','K'], ['cfs_num','$idnum'], ['cfs_pct','$precinct'], 
					['cfs_sector','B'], ['cfs_code','$code'], ['cfs_addr','$address'], ['cfs_body','$update'], ['cfs_finaldis','']]))");


				}

			}	
 		}

	} else {
		
		#for( ?.

		$jobsinarow++;
		$idnum++;
		
		push @jobarray, new Job($idnum);
		#$idnum = $jobarray[@jobIDs]->getID;
		$update = $jobarray[@jobIDs]->getUpdate();
		my $address = $jobarray[@jobIDs]->getAddress();
		
		#get data from jobarray before pushing new job, just so the index is right, otherwise it would be off by 1
		push(@jobIDs, "$idnum");
		push(@jobupdatewaits, int(rand(3))+1);
		
		my $precinct=selectPrecinct();
		$code=int(rand(88)+11);
		
							my $startime=strftime('%H:%M:%S', localtime);
	
		#Create random location
		$cfspoint=createRandomLocation($precinct, $dbh);
					
		#insert some location
		$insertlocations2 = "insert into location (source, has_data, data ,geometry) values(6, 't', hstore(ARRAY[['type','CFSDemo']]), St_Force_3D(St_GeomFromText( '$cfspoint', 4326) )   ) returning id";
		#print "$insertlocations2\n";
		$insertlocations2_handle = $dbh->prepare($insertlocations2);
		$insertlocations2_handle->execute();
		#get the returned id
		$locationid2 = $insertlocations2_handle->fetch()->[0];
		
		#my $rows = $dbh->do("INSERT INTO event (group_id, location, data) VALUES (999, $locationid2, hstore(ARRAY[['cfs_type','Fire'], ['cfs_letter','K'], ['cfs_num','$idnum'], ['cfs_pct','$precinct'], 
		#		['cfs_sector','B'], ['cfs_code','$code'], ['cfs_addr','$address'], ['cfs_body','$update'], ['cfs_finaldis','']]))");
		
		my $rows=InsertNewEvent();
	
		#If we reach the limit of jobs, then stop creating new jobs
		if(@jobIDs==$joblimit){$updatechance=1;}
	}

}#end big for loop

# clean up
#$dbh->disconnect();

#this selects a random job from the database and returns the cfs_num
sub getHighestID {
#	my $sth = $dbh->prepare("SELECT cfs_num from sr_cfs order by cfs_num desc limit 1;");
	my $sth = $dbh->prepare("SELECT data -> 'cfs_num' FROM event order by data -> 'cfs_num' desc limit 1;");
	$sth->execute();
	my @tmpdata = $sth->fetchrow_array();
	$sth->finish();
	
	return($tmpdata[0]);
}

#update a job number. Arguments: int cfs_num, string update. 
#not used currently
sub updateCFS {
	
	$num = $_[0];
	$text = $_[1];
}

sub selectPrecinct {

	#TODO - get numbers for fire station districts
	my @pctArr = (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36);
	return $pctArr[ int(rand(35) )] ;
}

sub jobtype {
	

}


sub InsertNewEvent(){

	#NOT FINISHED
		#Create random location
		
		$cfspoint=createRandomLocation($precinct, $dbh);
					
		#insert some location
		$insertlocations2 = "insert into location (source, has_data, data ,geometry) values(6, 't', hstore(ARRAY[['type','CFSDemo']]), St_Force_3D(St_GeomFromText( '$cfspoint', 4326) )   ) returning id";
		$insertlocations2_handle = $dbh->prepare($insertlocations2);
		$insertlocations2_handle->execute();
		#get the returned id
		$locationid2 = $insertlocations2_handle->fetch()->[0];
		my $newcfstype = $cfsTypeArr[ int(rand(2) )] ;
		
		my $rows = $dbh->do("INSERT INTO event (group_id, location, data) VALUES (999, $locationid2, hstore(ARRAY[['cfs_type','$newcfstype'], ['cfs_letter','K'], ['cfs_num','$idnum'], ['cfs_pct','$precinct'], 
				['cfs_sector','B'], ['cfs_code','$code'], ['cfs_addr','$address'], ['cfs_body','$update'], ['cfs_finaldis','']]))");
				
		return($rows);		

}

sub createRandomLocation($precinct, $dbh){
#it needs the precinct # and the connection to the database

#TODO Might need to change "PctName" field in db, or maybe just use it.
$selectpoint = "select st_astext(RandomPoint(geometry)) from srmap where data @> '\"PctName\"=>\"$precinct\"' and group_id=2052  limit 1";
$selectpoint_handle = $dbh->prepare($selectpoint);
$selectpoint_handle->execute();
$selectpoint_handle->bind_columns(undef, \$cfspoint);
$selectpoint_handle->fetch();
return($cfspoint);
}

