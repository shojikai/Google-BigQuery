use 5.008000;
use ExtUtils::MakeMaker;

WriteMakefile(
		NAME		=> 'Google::BigQuery',
		VERSION_FROM	=> 'lib/Google/BigQuery.pm',
		PREREQ_PM	=> {
					'DBI'				=> 1.30
				   },
		($] >= 5.005 ?     ## Add these new keywords supported since 5.005
					(AUTHOR		=> 'Daniel Kasak <d.j.kasak.dk@gmail.com>') : ()),
);
