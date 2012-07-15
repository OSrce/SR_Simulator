#!/usr/bin/perl 

package Job;
use Date::Calc qw(:all);

sub new
{
    my $class = shift;
    my $self = {
        _num => shift,
	_precinct => shift,
        _location  => undef,
        _type       => undef,
	_victim => undef,
	_perp => undef,
	_vehicle => undef,
	_updatenum => 0,
	@_updates => undef
    };
	
	#generate random data
	$self->{_type} = generateType();
	$self->{_victim} = generatePerson();
	$self->{_perp} = generatePerson();
	$self->{_location} = generateAddress();
	$self->{_vehicle} = generateVehicle();		
    # Print all the values just for clarification.
    print "New CFS #$self->{_num}: $self->{_type} at $self->{_location}\n";

    bless $self, $class;
    return $self;
}


sub setType {
    my ( $self, $type ) = @_;
    $self->{_type} = $type if defined($type);
    return $self->{_type};
}

sub generateType {

	my @crimes = ("Burglary", "Larceny", "Grand Theft Auto", "Assault", "Sexual Assault", "Homicide");

	return $crimes[int(rand(@crimes))];
}

sub generateVehicle {
        my @cars = qw(sedan pickup motorcycle van minivan sportscar SUV);
	my $car = $cars[int(rand(@cars))];
        my @colors = qw(white black dark green yellow red tan light silver);
	my $color = $colors[int(rand(@colors))];
        my $tmpchr = "";
        my @license = (int(rand(8)+1));

        #license plate known or unknown
#       if(rand()<0.5){

		#add 3 letters
                for($i=0;$i<3;$i++){
			$tmpchr=chr(int(rand(26)) + 65);
                	push(@license, $tmpchr);
		}
               push(@license, int(rand(8)+1) );
		push(@license, int(rand(8)+1) );
		push(@license, int(rand(8)+1) );
#       }
        my $licstr = join("",@license);
	return "Vehicle is a $color $car with license plate $licstr";
}
sub getVehicle {
	my( $self ) = @_;
    	return $self->{_vehicle};
}
sub getPrecinct {
	my( $self ) = @_;
    	return $self->{_precinct};
}

sub generateAddress {
	my $address = int(rand(1000))+45; 
	my @streets=("Broadway Ave", "Flatbush Ave", "Pelham Pkwy", "Lexington Ave", "Madison Ave", "Central Park West", "Houston Ave", "Bleecker St", "MacDougal St", "Astor Pl", "Wall St", "Park Ave", "Lenox Ave", "MLK Blvd", "St Nicholas Ave");
	
	if(rand()<0.3){
		$address .= " " . $streets[int(rand(@streets))];
	} elsif(rand()<0.4) {
		#choose random number ave
		my $streetnum = Date::Calc::English_Ordinal(int(rand(9) + 1));
		$address .= " " . $streetnum . " Ave";

	} else {
		#choose random number st
		my $streetnum = Date::Calc::English_Ordinal(int(rand(134) + 2));
		$address .= " " . $streetnum . " St";
	}

	return $address;
}
sub getAddress {
	my( $self ) = @_;
    	return $self->{_location};

}

sub getType {
    my( $self ) = @_;
    return $self->{_type};
}
sub getID {
    my( $self ) = @_;
    return $self->{_num};
}

sub setVictim {
    my ( $self, $victim ) = @_;
    $self->{_victim} = $victim if defined($victim);
    return $self->{_type};
}
sub getVictimDescription {
    my ( $self ) = @_;

    #my @description = ("$self->{_victim}->{_age}", "$self->{_victim}->{_race}", "$self->{_victim}->{_gender}");
        
	return "Victim is a $self->{_victim}->{_age} year old $self->{_victim}->{_race} $self->{_victim}->{_gender}";
}
sub getPerpDescription {
    my ( $self ) = @_;

    #my @description = ("$self->{_perp}->{_age}", "$self->{_perp}->{_race}", "$self->{_perp}->{_gender}");
        
    #    return (@description);
	return "Suspect is a $self->{_perp}->{_age} year old $self->{_perp}->{_race} $self->{_perp}->{_gender}";
}

sub generatePerson {
	my $age = int(rand(22))+15;
	my @race = ("white", "black", "black", "latino", "latino", "asian");
	my $gender = "male";
	if(rand() <0.3){ $gender="female";}
	my $temp = Person->new($age, $race[int(rand(@race))], $gender);
	return $temp;

}
1;
sub generateUpdates {
	my ( $self ) = @_;
	my $perpdesc = $self->getPerpDescription();
	my $vicdesc = $self->getVictimDescription();
	return ("New CFS #$self->{_num}: $self->{_type} at $self->{_location} . . .", "Unit assigned . . . ", "$perpdesc . . . ", "$self->{_vehicle} . . .", "$vicdesc . . . ", "Unit on scene . . .", "Case closed . . .");
	
}

sub getUpdate {
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

package Person;

sub new
{
    my $class = shift;
    my $self = {
        _age       => shift,
	_race => shift,
	_gender => shift
    };



	print "$self->{GENDER}";

    # Print all the values just for clarification.
    #print "First Name is $self->{_firstName}\n";
    #print "Last Name is $self->{_lastName}\n";

    bless $self, $class;
    return $self;
}
sub setFirstName {
    my ( $self, $firstName ) = @_;
    $self->{_firstName} = $firstName if defined($firstName);
    return $self->{_firstName};
}

sub getFirstName {
    my( $self ) = @_;
    return $self->{_firstName};
}
1;
