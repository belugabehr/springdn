package io.github.belugabehr.datanode.comms.nn;

import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.springframework.stereotype.Component;

@Component
public class NameNodeConnectionPoolFactory
    extends BaseKeyedPooledObjectFactory<URI, DatanodeProtocolClientSideTranslatorPB> {

  @Override
  public DatanodeProtocolClientSideTranslatorPB create(final URI key) throws Exception {
    final String hostname = key.getHost();
    final int port = key.getPort();

    final InetSocketAddress socket = new InetSocketAddress(hostname, port);
    final Configuration conf = new Configuration();
    return new DatanodeProtocolClientSideTranslatorPB(socket, conf);
  }

  @Override
  public PooledObject<DatanodeProtocolClientSideTranslatorPB> wrap(DatanodeProtocolClientSideTranslatorPB value) {
    return new DefaultPooledObject<>(value);
  }

}
