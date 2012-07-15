#!/usr/bin/perl

# load module
use DBI;
use Job;
use POSIX qw/strftime/;



# connect to database
my $dbh = DBI->connect("DBI:Pg:dbname=sr_data;host=localhost", "sitrepadmin", "", {'RaiseError' => 1});
#my $dbh = DBI->connect("DBI:Pg:dbname=sr_data;host=localhost", "alexkorb", "secret", {'RaiseError' => 1});

my @precints=qw(1 5 6 7 9 10 13 17 19 20 23 24 25 26 28 30 32 33 34);
#start with a random CFS
@jobarray=("");
$jobarray[0] = new Job( int(rand(1000)));
$idnum = $jobarray[0]->getID;

#update cfs_body in database
my $update = $jobarray[0]->getUpdate();
my $date=strftime('%Y-%d-%m',localtime); 
my $code=int(rand(88)+11);
my $rows = $dbh->do("INSERT INTO sr_cfs (cfs_date, cfs_letter, cfs_num, cfs_pct, cfs_sector, cfs_code, cfs_addr, cfs_body) VALUES ('$date', 'K', '$idnum', '23', 'B', $code, '100 Test Street', '$update')");
#for pseudorandomness to make sure not too many jobs are created in a row
my $jobsinarow=1;

@jobIDs=("$idnum"); #list of job IDs

my $joblimit=10; #limit the total number of jobs in the simulation
my $MaxTimeTilNextUpdate=5; #each job will be updated between 1 and 1 + MaxTimeTilNextUpdate seconds
my $updatechance=0.7; #likelihood of updating an existing CFS vs starting a new CFS
my $numjobs=1;

#my @updatecounts = (0) x $joblimit; #The number of times a specific job has been updated; it's length is the number of possible jobs
#my @jobupdatewaits=(0) x $joblimit; #The amount of time to wait before next updating each job
#$jobupdatewaits[0]=3;

my @updatecounts = ("0");
my @jobupdatewaits= ("3");


#This iterates through either updating or starting a new job, and waits 1 sec each time.
for($iterations = 0; $iterations<30; $iterations++ ){
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
				$idnum = $jobarray[$i]->getID;
				my $tmpupdate = $jobarray[$i]->getUpdate();
				print "Update $updatecounts[$i] for job $jobIDs[$i]: $tmpupdate\n";
				my $rows = $dbh->do("UPDATE sr_cfs set cfs_body = cfs_body || '$tmpupdate' where cfs_num=$idnum");
				#determine a time when this job will be updated next
				$jobupdatewaits[$i] = int(rand($MaxTimeTilNextUpdate)) + 1;
			}	
 		}

	} else {
		#choose a random job number
		$jobsinarow++;
		$newjob=1;
		#check to see if it matches an existing job
		while($newjob==1){
			#choose another job					
			$newjob=0;
			$idnum = int(rand(1000));
			#check to see if the new job matches one already in the array
			foreach (@jobIDs) {
			 	if ($idnum eq $_) { $newjob=1;}
 			} 	
		}#end while new job
		
		push @jobarray, new Job($idnum);
		#$idnum = $jobarray[@jobIDs]->getID;
		$update = $jobarray[@jobIDs]->getUpdate();
		my $address = $jobarray[@jobIDs]->getAddress();
		
		#get data from jobarray before pushing new job, just so the index is right, otherwise it would be off by 1
		push(@jobIDs, "$idnum");
		push(@jobupdatewaits, int(rand(3))+1);
		
		my $precint=$precints[int(rand(@precints))];
		my $code=int(rand(88)+11);
		
		my $rows = $dbh->do("INSERT INTO sr_cfs (cfs_date, cfs_letter, cfs_num, cfs_pct, cfs_sector, cfs_code, cfs_addr, cfs_body) 
				VALUES ('$date', 'K', '$idnum', $precint, 'B', $code, '$address', '$update')");
		
		#If we reach the limit of jobs, then stop creating new jobs
		if(@jobIDs==$joblimit){$updatechance=1;}
	}

}#end big for loop

# clean up
#$dbh->disconnect();

#this selects a random job from the database and returns the cfs_num
sub getRandomCFS {
	my $sth = $dbh->prepare("SELECT cfs_num from sr_cfs order by RANDOM() limit 1");
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


sub jobtype {
	

}


