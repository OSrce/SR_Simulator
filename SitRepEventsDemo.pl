#!/usr/bin/perl

# load module
use DBI;
use Job;
use POSIX qw/strftime/;



# connect to database
my $dbh = DBI->connect("DBI:Pg:dbname=sr_data;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});
#my $dbh = DBI->connect("DBI:Pg:dbname=sr_data;host=localhost", "alexkorb", "secret", {'RaiseError' => 1});

#start with a random CFS
@jobarray=("");
my $precinct=selectPrecinct();
$jobarray[0] = new Job(getHighestID()+1, $precinct);
$idnum = $jobarray[0]->getID;

#update cfs_body in database
my $update = $jobarray[0]->getUpdate();
my $date=strftime('%Y-%d-%m',localtime); 
my $code=int(rand(8)+31);
my $startime=strftime('%H:%M:%S', localtime);
my $address = $jobarray[@jobIDs]->getAddress();

my $rows = $dbh->do("INSERT INTO sr_cfs (cfs_date, cfs_letter, cfs_num, cfs_pct, cfs_sector, cfs_code, cfs_addr, cfs_body, cfs_timecreated) VALUES ('$date', 'K', '$idnum', '$precinct', 'B', $code, '1
00 Test Street', '$update', '$startime')");



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
				my $rows = $dbh->do("UPDATE sr_cfs set cfs_body = cfs_body || '$tmpupdate' where cfs_num=$tmpidnum");
				#determine a time when this job will be updated next
				$jobupdatewaits[$i] = int(rand($MaxTimeTilNextUpdate)) + 1;

				#maybe change this to asking the job if it's done
				if($updatecounts[$i]>6){
					#then close the job	
					print "Job #$tmpidnum is closing.\n";
					#add final disposition, time etc.	
					my $finaldis=int(rand(8)) +91;
					my $finaldisdate=strftime('%Y-%m-%d %H:%M:%S', localtime);
					my $rows = $dbh->do("UPDATE sr_cfs set cfs_finaldis = '$finaldis', cfs_finaldisdate = '$finaldisdate' where cfs_num=$tmpidnum");
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

					my $rows = $dbh->do("INSERT INTO sr_cfs (cfs_date, cfs_letter, cfs_num, cfs_pct, cfs_sector, cfs_code, cfs_addr, cfs_body, cfs_timecreated) 
							VALUES ('$date', 'K', '$idnum', '$precinct', 'B', $code, '$address', '$update', '$startime')");
		
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

		my $rows = $dbh->do("INSERT INTO sr_cfs (cfs_date, cfs_letter, cfs_num, cfs_pct, cfs_sector, cfs_code, cfs_addr, cfs_body, cfs_timecreated) 
				VALUES ('$date', 'K', '$idnum', '$precinct', 'B', $code, '$address', '$update', '$startime')");
		

		#If we reach the limit of jobs, then stop creating new jobs
		if(@jobIDs==$joblimit){$updatechance=1;}
	}

}#end big for loop

# clean up
#$dbh->disconnect();

#this selects a random job from the database and returns the cfs_num
sub getHighestID {
	my $sth = $dbh->prepare("SELECT cfs_num from sr_cfs order by cfs_num desc limit 1;");
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
	return int(rand(76) );
#	my $sth = $dbh->prepare("SELECT cfs_pct FROM sr_cfs_backup ORDER BY RANDOM() limit 1;");
#	$sth->execute();
#	my @tmpdata = $sth->fetchrow_array();
#	$sth->finish();
	
#	return($tmpdata[0]);
}

sub jobtype {
	

}
