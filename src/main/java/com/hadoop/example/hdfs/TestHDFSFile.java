package com.island.ohara.hdfs

import com.island.ohara.rule.MediumTest
import org.junit.Test
import org.scalatest.Matchers
import java.util

import com.island.ohara.hdfs.storage.HDFSStorage

class TestHDFSStorage extends MediumTest with Matchers {
  private val HDFS_NAMENODE_URL = "hdfs://hdfs:9000"

  @Test
  def testListFile(): Unit = {
    val props: util.Map[String, String] = new util.HashMap[String, String]
    props.put(HDFSSinkConnectorConfig.HDFS_URL, HDFS_NAMENODE_URL)

    val config = new HDFSSinkConnectorConfig(props)
    val hdfsStorage = new HDFSStorage(config)
    val result = hdfsStorage.list("/")
    result.foreach(x => {
      println(x)
    })
  }

  @Test
  def testOpen(): Unit = {
    val props: util.Map[String, String] = new util.HashMap[String, String]
    props.put(HDFSSinkConnectorConfig.HDFS_URL, HDFS_NAMENODE_URL)

    val config = new HDFSSinkConnectorConfig(props)
    val hdfsStorage = new HDFSStorage(config)
    val file = hdfsStorage.open("/aaa123.txt", true)
    file.write("HELLO WORLD 1\n".getBytes())
    file.write("HELLO WORLD 2\n".getBytes())
    file.close()
  }

  @Test
  def testAppend(): Unit = {
    val props: util.Map[String, String] = new util.HashMap[String, String]
    props.put(HDFSSinkConnectorConfig.HDFS_URL, HDFS_NAMENODE_URL)

    val config = new HDFSSinkConnectorConfig(props)
    val hdfsStorage = new HDFSStorage(config)
    val file = hdfsStorage.append("/aaa123.txt")
    file.write("HELLO WORLD 3\n".getBytes())
    file.close()
  }

  @Test
  def testMkdir(): Unit = {
    val props: util.Map[String, String] = new util.HashMap[String, String]
    props.put(HDFSSinkConnectorConfig.HDFS_URL, HDFS_NAMENODE_URL)

    val config = new HDFSSinkConnectorConfig(props)
    val hdfsStorage = new HDFSStorage(config)
    hdfsStorage.mkdir("/test123456")
  }

  @Test
  def testExists(): Unit = {
    val props: util.Map[String, String] = new util.HashMap[String, String]
    props.put(HDFSSinkConnectorConfig.HDFS_URL, HDFS_NAMENODE_URL)

    val config = new HDFSSinkConnectorConfig(props)
    val hdfsStorage = new HDFSStorage(config)
    println(hdfsStorage.exists("/test123456"))
    println(hdfsStorage.exists("/test1234567"))
  }

  @Test
  def testDelete(): Unit = {
    val props: util.Map[String, String] = new util.HashMap[String, String]
    props.put(HDFSSinkConnectorConfig.HDFS_URL, HDFS_NAMENODE_URL)

    val config = new HDFSSinkConnectorConfig(props)
    val hdfsStorage = new HDFSStorage(config)
    val folderName = "/test123456"
    hdfsStorage.mkdir(folderName)
    println(hdfsStorage.exists(folderName))
    hdfsStorage.delete(folderName)
    println(hdfsStorage.exists(folderName))
  }

  @Test
  def testRenameFile(): Unit = {
    val props: util.Map[String, String] = new util.HashMap[String, String]
    props.put(HDFSSinkConnectorConfig.HDFS_URL, HDFS_NAMENODE_URL)

    val config = new HDFSSinkConnectorConfig(props)
    val hdfsStorage = new HDFSStorage(config)

    hdfsStorage.renameFile("/aaaa000", "/bbbb000")
  }
}
