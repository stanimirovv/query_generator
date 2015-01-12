use strict;
use warnings;
use Data::Dumper;
use DBI;
 
=pod
my $schema =
{
 blue => {red => {is_nullable => 0, column => "red_id"}},
 red => {pink => {is_nullable => 0, column => "pink_id"}, green => {is_nullable => 1, column => "green_id"}},
 pink => {black => {is_nullable => 0, column => "black_id"}},
 black => {white => {is_nullable => 1, column => "white_id"}},
 green => {green => {is_nullable => 0, column => "green_id"}, black => {is_nullable => 1, column => "black_id"}, yellow => {is_nullable => 0, column => "yellow_id"}},
 yellow => {brown => {is_nullable => 0, column => "brown_id"}, cian => {is_nullable => 0, column => "cian_id"}, orange => {is_nullable => 1, column => "orange_id"}},
 white => {blue => {is_nullable => 1, column => "blue_id"}}
};
=cut

sub CreateSchemaHash()
{
    my $dbh = DBI->connect('dbi:Pg:dbname=querygenerator;host=localhost','postgres','123',{AutoCommit=>1,RaiseError=>1,PrintError=>0});
 
    my $sth = $dbh->prepare("SELECT * FROM information_schema.columns where table_catalog='querygenerator' AND table_schema='public'");
    $sth->execute();
 
    $sth = $dbh->prepare("
         SELECT
             tc.constraint_name, tc.table_name, kcu.column_name,
          ccu.table_name AS foreign_table_name,
          ccu.column_name AS foreign_column_name,
          cl.is_nullable
          FROM
              information_schema.table_constraints AS tc
              JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
              JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
              JOIN information_schema.columns AS cl
                  ON cl.column_name = kcu.column_name AND cl.table_name = kcu.table_name
          WHERE constraint_type = 'FOREIGN KEY';
      ");
    $sth->execute();
 
    my $hash = {};
 
    while(my $row = $sth->fetchrow_hashref())
    {
        if(!defined $$hash{ $$row{table_name} })
        {
            $$hash{ $$row{table_name} } = {};
        }
 
        $$hash{ $$row{table_name} }{ $$row{foreign_table_name} } = {is_nullable => 1, column => $$row{column_name}};
   
        if( $$row{is_nullable} eq "NO")
        {
            $$hash{ $$row{table_name} }{ $$row{foreign_table_name} }{is_nullable} = 0;
        }
 
    }
 
    return $hash;
}
 
#my $routes = {};
sub GraphTraversalHelper
{
    my ($schema, $visited_history, $cycles, $max_rotations, $joins, $joins_l, $start_from, $is_ljoin, $router, $router_hash) = @_;
   
    my $routes = scalar keys(%{$$schema{$start_from}});
    if($routes >= 2)
    {
        $router .= $start_from;
    }
 
    my @sortedTablesJoin;
    my @sortedTablesLeft;
    for my $table (keys(%{$$schema{$start_from}}))
    {
        if($$schema{$start_from}{$table}{is_nullable} == 0){
            push @sortedTablesJoin, $table;
        }else{
            push @sortedTablesLeft, $table;
        }
    }
 
    my @sortedTables = (@sortedTablesJoin, @sortedTablesLeft);
     
    #for my $points_to (keys(%{$$schema{$start_from}}))
 
    foreach my $points_to (@sortedTables)
    {
 
        #print "Points to: $points_to \n";
        my $is_ljoin_new = 0;
       
        if(!defined($$cycles{"$start_from-$points_to"}))
        {
            $$cycles{"$start_from-$points_to"} = 0;
        }
        $$cycles{"$start_from-$points_to"}++;
       
        if($$cycles{"$start_from-$points_to"} > $max_rotations)
        {
            return;
        }
        if(!defined($$visited_history{$points_to}))
        {
            $$visited_history{$points_to} = 0;
        }
        if(!defined($$visited_history{$start_from}))
        {
            $$visited_history{$start_from} = 1;
        }
        $$visited_history{$points_to}++;
        my $cycle = $$cycles{"$start_from-$points_to"};
 
        #joins
        my $alias = $router."_".$points_to;
        # my $alias2 = $prev_router."_".$start_from;
        $$router_hash{ $points_to } = $router;  
        my $alias2;
        if(defined $$router_hash{ $start_from })
        {
            $alias2 = $$router_hash{ $start_from }."_".$start_from;
        }else{
            $alias2 = "_".$start_from;
        }
 
        if($$schema{$start_from}{$points_to}{is_nullable} == 0 && $is_ljoin == 0)
        {
            $is_ljoin_new = 0;
           # print " JOIN $points_to AS $alias  ON $alias.id = $alias2.$$schema{$start_from}{$points_to}{column}\n";
            push @$joins, " JOIN $points_to AS $alias  ON $alias.id = $alias2.$$schema{$start_from}{$points_to}{column}";
        }
        else
        {
            #print " LEFT JOIN $points_to AS $alias ON $alias.id = $alias2.$$schema{$start_from}{$points_to}{column}\n";
            $is_ljoin_new = 1;
            push @$joins_l, " LEFT JOIN $points_to AS $alias ON $alias.id = $alias2.$$schema{$start_from}{$points_to}{column}";
        }
        GraphTraversalHelper($schema, $visited_history, $cycles, $max_rotations, $joins, $joins_l, $points_to, $is_ljoin_new, $router, $router_hash);
    }
}
 
sub GraphTraversal($$$)
{
    my ($schema, $start_from, $iterations) = @_;
   
    my $joins = [];
    my $joins_l = [];
    GraphTraversalHelper($schema, {$start_from => 1}, {}, $iterations, $joins, $joins_l, $start_from, 0, "", {});
       
    return "SELECT * FROM $start_from AS _$start_from". join(" ", @$joins) . join(" ", @$joins_l);
}
 
 
sub Main()
{
    my $schema = CreateSchemaHash();
    print GraphTraversal($schema, "blue", 3), "\n";
}
 
Main();
