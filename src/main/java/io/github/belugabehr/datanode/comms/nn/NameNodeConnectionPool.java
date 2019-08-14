package io.github.belugabehr.datanode.comms.nn;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.NoSuchElementException;

import javax.annotation.PostConstruct;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.hadoop.hdfs.protocolPB.DatanodeProtocolClientSideTranslatorPB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class NameNodeConnectionPool implements KeyedObjectPool<URI, DatanodeProtocolClientSideTranslatorPB> {

  @Autowired
  private NameNodeConnectionPoolFactory connectionPoolFactory;

  private GenericKeyedObjectPool<URI, DatanodeProtocolClientSideTranslatorPB> objectPool;

  public NameNodeConnectionPool() {
  }

  @PostConstruct
  public void init() throws URISyntaxException {
    this.objectPool = new GenericKeyedObjectPool<>(this.connectionPoolFactory);
    this.objectPool.setMaxIdlePerKey(1);
    this.objectPool.setMaxTotalPerKey(1);
  }

  @Override
  public DatanodeProtocolClientSideTranslatorPB borrowObject(URI key)
      throws Exception, NoSuchElementException, IllegalStateException {
    return this.objectPool.borrowObject(key);
  }

  @Override
  public void returnObject(URI key, DatanodeProtocolClientSideTranslatorPB obj) {
    if (obj != null) {
      this.objectPool.returnObject(key, obj);
    }
  }

  @Override
  public void invalidateObject(URI key, DatanodeProtocolClientSideTranslatorPB obj) throws Exception {
    this.objectPool.invalidateObject(key, obj);
  }

  @Override
  public void addObject(URI key) throws Exception, IllegalStateException, UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getNumIdle(URI key) {
    return this.objectPool.getNumIdle(key);
  }

  @Override
  public int getNumActive(URI key) {
    return this.objectPool.getNumActive(key);
  }

  @Override
  public int getNumIdle() {
    return this.objectPool.getNumIdle();
  }

  @Override
  public int getNumActive() {
    return this.objectPool.getNumActive();
  }

  @Override
  public void clear() throws Exception, UnsupportedOperationException {
    this.objectPool.clear();
  }

  @Override
  public void clear(URI key) throws Exception, UnsupportedOperationException {
    this.objectPool.clear(key);
  }

  @Override
  public void close() {
    this.objectPool.close();
  }
}
