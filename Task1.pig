
Data_file = LOAD '/home/acadgild/Pig/DelayedFlights.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','UNIX','SKIP_INPUT_HEADER');
columns = FOREACH Data_file GENERATE (int)$1 AS year,(int)$10 AS flight_num,(chararray)$17 AS origin,(chararray)$18 AS desti ;destination = FILTER columns BY desti is not null;
group_desti = GROUP destination BY desti;
count_group = FOREACH group_desti GENERATE group, COUNT(destination.desti);
order_group = ORDER count_group BY $1 DESC;
limit_group = LIMIT order_group 5;
dump limit_group;

