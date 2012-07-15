#!/bin/sh

cp SitRepEventsDemo_Object.pl tmpfile.pl
more tmpfile.pl | sed s/alexkorb/sitrepadmin/g | sed s/secret//g > SitRepEventsDemo_Object.pl
rm tmpfile.pl
