/**
 * Stores sstable format. A sstable file is partitioned into
 * multiple shards. Each shard is further indexed by position
 * of blocks in SSTable. Each block is typically 512KB and is
 * smallest unit of data. This index is stored in the header
 * of SSTable.
 * TODO(Nikunj): SSTable hadoop module. (https://github.com/ifesdjeen/cassandra-hadoop-2/blob/master/src/main/java/org/apache/cassandra/hadoop2/BulkOutputFormat.java)
 */
public class SSTable extends File {

}