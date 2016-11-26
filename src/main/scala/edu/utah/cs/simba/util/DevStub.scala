package edu.utah.cs.simba.util

/**
  * Created by gefei on 2016/11/11.
  * This Object stores the necessary parameter for this project
  * This is only use for temporary developmental usage. Will be merged into SimbaContext later.
  */
object DevStub {
  def numShuffledPartitions: Int = 200
  def maxEntriesPerNode: Int = 25
  def sampleRate: Double = 0.01
  def transferThreshold: Int = 800 * 1024 * 1024
  def partitionMethod: String = "STRPartitioner"
  def defaultSizeInBytes: Int = 10 * 1024 * 1024 + 1
}
