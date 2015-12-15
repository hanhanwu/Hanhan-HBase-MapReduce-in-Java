# HBase-MapReduce-in-Java
The code in this folder uses HBase and Hadoop MapReduce in Java

LoadLogs.java

This file will send data into HBase through Java standalone program. The input data is from NASA serve logs.

get_put() function returns a Put object that will insert this log entry. The function will just create a row key, and put the string to raw:line

We need a row key that is deterministic and uniformly-distributed over the possible byte strings. Good thing somebody invented hash functions. We will use the MD5 hash of the line. There is a DigestUtils class included with Hadoop that will do this for us

import org.apache.commons.codec.digest.DigestUtils;

byte[] rowkey = DigestUtils.md5(line);

Everything you give to HBase must be byte arrays: it's very literal about what it stores for you. You end up writing a lot of code using the HBase Bytes class.

Sample NASA data input:

in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839

uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] "GET / HTTP/1.0" 304 0

uplherc.upl.com - - [01/Aug/1995:00:00:08 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 304 0


LoadLogsMR.java

Writing a simple standalone program that populates HBase is often useful, but if you have genuinely big data, then it might not be practical. This file is using Map Reduce to send larger amount of log data into HBase.


CorrelateLogs.java

My professor has a theory, he thinks, that in our web server logs, the number of bytes transferred to a host might be correlated with the number of requests the host makes. He thinks this is a good theory. In order to check this theory, I would like to calculate the correlation coefficient of each host's number of requests and total bytes transferred. That is, each host making requests will contribute a data point (x,y) where x is the number of requests made, and y is the number of bytes transferred. Then the correlation coefficient can be calculated as, https://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient#For_a_sample

The mapper gets the data from the logs from HBase, the reducer sums the count and bytes for each host. In order to squeeze an extra step into MapReduce, we can use the ChainReducer, which allows us to hook an extra mapper up to work on the reducer's output. The AggregateMapper aggregates all of the data points after the reducer runs.


