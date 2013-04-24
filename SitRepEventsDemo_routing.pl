#!/usr/bin/perl

# load module
use DBI;
use Job_routing;
use POSIX qw/strftime/;



# connect to database
my $dbh = DBI->connect("DBI:Pg:dbname=sitrepdev;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});

#start with a random CFS
#@jobarray=("");
@jobarray;
$idnum =getHighestID()+1;

my @cfsTypeArr = ("Fire","Medical", "Fire");
	

#@jobIDs=("$idnum"); #list of job IDs
#@jobIDs=(""); #list of job IDs
@jobIDs;

#for pseudorandomness to make sure not too many jobs are created in a row
my $jobsinarow=1;

my $joblimit=15; #limit the total number of jobs in the simulation
my $MaxTimeTilNextUpdate=50; #each job will be updated between 1 and 1 + MaxTimeTilNextUpdate seconds
my $updatechance=0.85; #likelihood of updating an existing CFS vs starting a new CFS
my $numjobs=1;

my @updatecounts = (0) x $joblimit; #The number of times a specific job has been updated; it's length is the number of possible jobs
my @jobupdatewaits=(0) x $joblimit; #The amount of time to wait before next updating each job
$jobupdatewaits[0]=3;


my $rows=InsertNewEvent(-1);

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
			$tmpidnum = $jobarray[$i]->getID;
 			if($jobupdatewaits[$i]<1){
				$jobsinarow=0;
				#increase the counter for number of updates on a particular job
				if($updatecounts[$i]<=4){
					$updatecounts[$i]++;
					
					my $tmpupdate = $jobarray[$i]->getUpdate();
					print "Update $updatecounts[$i] for job $jobIDs[$i]: $tmpupdate\n";
					
					my $rows = $dbh->do("UPDATE event set data = data || hstore('cfs_body', data->'cfs_body' || '$tmpupdate') where data @> '\"cfs_num\"=>\"$tmpidnum\"'");
									
					#determine a time when this job will be updated next
				
					$jobupdatewaits[$i] = int(rand($MaxTimeTilNextUpdate)) + 1;

					#maybe change this to asking the job if it's done
					
				} 
			}	
 		
			#This just checks if the finaldis has been set and the event has been ended
			#my $rows = $dbh->do("update event set data_end=data_end where data @> '\"cfs_num\"=>\"$tmpidnum\"' AND data->'cfs_finaldis' !=''");						  
			
			
			$eventid="";
			#Find out if the unit is enroute or on scene or not
			$endquery = "select id from event where data @> '\"cfs_num\"=>\"$tmpidnum\"' AND data->'cfs_finaldis' !='' AND has_end='t' limit 1";											  
			$endquery_handle = $dbh->prepare($endquery);
			$endquery_handle->execute();
			$endquery_handle->bind_columns(undef, \$eventid);
			$endquery_handle->fetch();

			if($eventid ne ""){
				print "Ending event $eventid\n";
				#replace the job with a new job
				$idnum++;
				$jobupdatewaits[$i] = int(rand($MaxTimeTilNextUpdate)) + 1;
				$updatecounts[$i]=0;
				my $rows=InsertNewEvent($i);
			}
 		}
	} else {
		
		$jobsinarow++;
		$idnum++;
		
		my $rows=InsertNewEvent(-1);
	
		#If we reach the limit of jobs, then stop creating new jobs
		if(@jobIDs==$joblimit){$updatechance=1;}
	}

}#end big for loop

# clean up
#$dbh->disconnect();

#this selects a random job from the database and returns the cfs_num
sub getHighestID {
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

	#The numbers from LA come from the LA city planning website for Community Planning Areas (not city council districts)
	#14 was removed from the list because it kept giving routing errors. It's the area around LAX ... and also 3 apparently
	my @pctArr = (1,2,4,5,6,7,8,9,10,11,12,13,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36);
	return $pctArr[ int(rand(35) )] ;
}

sub jobtype {
	

}


#howtoinsert is negative if you want the job pushed on, or the array index if you want the job inserted at a particular location in the array
sub InsertNewEvent(){

		my($howtoinsert) = @_;

		my $newcfstype = $cfsTypeArr[ int(rand(2) )] ;

		if($howtoinsert<0){
			push @jobarray, new Job($idnum, $newcfstype);
			$update = $jobarray[@jobIDs]->getUpdate();
			my $address = $jobarray[@jobIDs]->getAddress();
		
			#get data from jobarray before pushing new job, just so the index is right, otherwise it would be off by 1
			push(@jobIDs, "$idnum");
			push(@jobupdatewaits, int(rand(3))+1);
		} else {
				#I don't think precinct is neccessary
			#$jobarray[$howtoinsert] = new Job($idnum, $precinct);
			$jobarray[$howtoinsert] = new Job($idnum, $newcfstype);
			$jobupdatewaits[$howtoinsert] = int(rand($MaxTimeTilNextUpdate)) + 1;
			$updatecounts[$howtoinsert]=0;
		
		}
		
		
		$code=int(rand(88)+11);
		my $startime=strftime('%H:%M:%S', localtime);
	
		#Create random location
		$precinct=selectPrecinct();
		
		$cfspoint=createRandomLocation($precinct, $dbh);
					
		#insert some location
		$insertlocations2 = "insert into location (source, has_data, data ,geometry) values(6, 't', hstore(ARRAY[['type','CFSDemo']]), St_Force_3D(St_GeomFromText( '$cfspoint', 4326) )   ) returning id";
		$insertlocations2_handle = $dbh->prepare($insertlocations2);
		$insertlocations2_handle->execute();
		#get the returned id
		$locationid2 = $insertlocations2_handle->fetch()->[0];
		
		my $rows = $dbh->do("INSERT INTO event (group_id, location, data) VALUES (999, $locationid2, hstore(ARRAY[['cfs_type','$newcfstype'], ['cfs_letter','K'], ['cfs_num','$idnum'], ['cfs_pct','$precinct'], 
				['cfs_sector','B'], ['cfs_code','$code'], ['cfs_addr','$address'], ['cfs_body','$update'], ['cfs_finaldis','']]))");
				
		return($rows);		

}

sub createRandomLocation(){
#it needs the precinct # and the connection to the database

#TODO Might need to change "PctName" field in db, or maybe just use it.
$selectpoint = "select st_astext(RandomPoint(geometry)) from srmap where data @> '\"PctName\"=>\"$precinct\"' and group_id=2052  limit 1";
$selectpoint_handle = $dbh->prepare($selectpoint);
$selectpoint_handle->execute();
$selectpoint_handle->bind_columns(undef, \$cfspoint);
$selectpoint_handle->fetch();
return($cfspoint);
}

