/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.datasketches.hll.{HllSketch, TgtHllType, Union}
import org.apache.datasketches.memory.WritableMemory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types.{AbstractDataType, ArrayType, BinaryType, ByteType, DataType, DoubleType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

sealed trait HllSketchAggregate
  extends TypedImperativeAggregate[Array[Byte]] with UnaryLike[Expression] {

  def lgConfigK: Int
  def tgtHllType: String
  def getTgtHllType(): TgtHllType = {TgtHllType.valueOf(tgtHllType)}
  def useHeap: Boolean

  def byteArrayToHllSketch(buffer: Array[Byte]): HllSketch = {
    if (useHeap) {
      HllSketch.heapify(buffer)
    } else {
      HllSketch.writableWrap(WritableMemory.writableWrap(buffer))
    }
  }

  override def nullable: Boolean = false

  /**
   * Creates an empty aggregation buffer object. This is called before processing each key group
   * (group by key).
   *
   * @return an aggregation buffer object
   */
  override def createAggregationBuffer(): Array[Byte] = {
    if (useHeap) {
      val sketch = new HllSketch(lgConfigK, getTgtHllType())
      sketch.toUpdatableByteArray
    } else {
      val bufferSize = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, getTgtHllType())
      val buffer = new Array[Byte](bufferSize)
      new HllSketch(lgConfigK, getTgtHllType(), WritableMemory.writableWrap(buffer))
      buffer
    }
  }

  /**
   * Updates the aggregation buffer object with an input row and returns a new buffer object. For
   * performance, the function may do in-place update and return it instead of constructing new
   * buffer object.
   *
   * This is typically called when doing Partial or Complete mode aggregation.
   *
   * @param buffer The aggregation buffer object.
   * @param input  an input row
   */
  override def update(buffer: Array[Byte], input: InternalRow): Array[Byte] = {
    val sketch = byteArrayToHllSketch(buffer)
    val v = child.eval(input)
    if (v != null) {
      child.dataType match {
        // Implement update for all types supported by HllSketch
        // Spark doesn't have equivalent types for ByteBuffer or char[] so leave those out
        // TODO: Test all supported types, figure out if we should support all other SQL types
        // via direct calls to HllSketch.couponUpdate() ?
        case IntegerType => sketch.update(v.asInstanceOf[Int])
        case LongType => sketch.update(v.asInstanceOf[Long])
        case DoubleType => sketch.update(v.asInstanceOf[Double])
        case StringType => sketch.update(v.asInstanceOf[UTF8String].toString)
        case ArrayType(ByteType, _) => sketch.update(v.asInstanceOf[Array[Byte]])
        case ArrayType(IntegerType, _) => sketch.update(v.asInstanceOf[Array[Int]])
        case ArrayType(LongType, _) => sketch.update(v.asInstanceOf[Array[Long]])
        case _ => throw new UnsupportedOperationException()
      }
    }

    if (useHeap) {
      sketch.toUpdatableByteArray
    } else {
      buffer
    }
  }

  /**
   * Merges an input aggregation object into aggregation buffer object and returns a new buffer
   * object. For performance, the function may do in-place merge and return it instead of
   * constructing new buffer object.
   *
   * This is typically called when doing PartialMerge or Final mode aggregation.
   *
   * @param buffer the aggregation buffer object used to store the aggregation result.
   * @param input  an input aggregation object. Input aggregation object can be produced by
   *               de-serializing the partial aggregate's output from Mapper side.
   */
  override def merge(buffer: Array[Byte], input: Array[Byte]): Array[Byte] = {
    val sketch1 = HllSketch.wrap(WritableMemory.writableWrap(buffer))
    val sketch2 = HllSketch.wrap(WritableMemory.writableWrap(input))

    if (!useHeap && sketch1.getTgtHllType == TgtHllType.HLL_8) {
      val union = Union.writableWrap(WritableMemory.writableWrap(buffer))
      union.update(sketch2)
      buffer
    } else {
      val union = Union.heapify(buffer)
      union.update(sketch2)
      union.getResult(sketch1.getTgtHllType).toUpdatableByteArray
    }
  }

  /** Serializes the aggregation buffer object T to Array[Byte] */
  override def serialize(buffer: Array[Byte]): Array[Byte] = {
    buffer
  }

  /** De-serializes the serialized format Array[Byte], and produces aggregation buffer object T */
  override def deserialize(storageFormat: Array[Byte]): Array[Byte] = {
    storageFormat
  }
}

case class HllSketchEstimate(
    child: Expression,
    lgConfigK: Int = HllSketch.DEFAULT_LG_K,
    tgtHllType: String = HllSketch.DEFAULT_HLL_TYPE.toString,
    useHeap: Boolean = true,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends HllSketchAggregate {

  override def prettyName: String = "hllsketch_estimate"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def dataType: DataType = LongType

  override protected def withNewChildInternal(newChild: Expression):
    HllSketchEstimate = copy(child = newChild)

  /**
   * Generates the final aggregation result value for current key group with the aggregation buffer
   * object.
   *
   * Developer note: the only return types accepted by Spark are:
   *   - primitive types
   *   - InternalRow and subclasses
   *   - ArrayData
   *   - MapData
   *
   * @param buffer aggregation buffer object.
   * @return The aggregation result of current key group
   */
  override def eval(buffer: Array[Byte]): Any = {
    HllSketch.wrap(WritableMemory.writableWrap(buffer)).getEstimate.toLong
  }
}

case class HllSketchBinary(
    child: Expression,
    lgConfigK: Int = HllSketch.DEFAULT_LG_K,
    tgtHllType: String = HllSketch.DEFAULT_HLL_TYPE.toString,
    useHeap: Boolean = true,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends HllSketchAggregate {

  override def prettyName: String = "hllsketch_byte_array"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def dataType: DataType = BinaryType

  override protected def withNewChildInternal(newChild: Expression):
    HllSketchBinary = copy(child = newChild)

  /**
   * Generates the final aggregation result value for current key group with the aggregation buffer
   * object.
   *
   * Developer note: the only return types accepted by Spark are:
   *   - primitive types
   *   - InternalRow and subclasses
   *   - ArrayData
   *   - MapData
   *
   * @param buffer aggregation buffer object.
   * @return The aggregation result of current key group
   */
  override def eval(buffer: Array[Byte]): Any = {
    buffer
  }
}

case class HllSketchBinaryEstimate(
    child: Expression,
    lgMaxK: Int = HllSketch.DEFAULT_LG_K,
    useHeap: Boolean = true,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Array[Byte]]
  with UnaryLike[Expression]
  with ImplicitCastInputTypes {

  override def prettyName: String = "hllsketch_byte_array_estimate"

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def dataType: DataType = LongType

  override protected def withNewChildInternal(newChild: Expression):
    HllSketchBinaryEstimate = copy(child = newChild)

  override def nullable: Boolean = false

  def byteArrayToUnion(buffer: Array[Byte]): Union = {
    if (useHeap) {
      Union.heapify(buffer)
    } else {
      Union.writableWrap(WritableMemory.writableWrap(buffer))
    }
  }

  def unionToByteArray(union: Union, buffer: Array[Byte]): Array[Byte] = {
    if (useHeap) {
      union.toUpdatableByteArray
    } else {
      buffer
    }
  }

  /**
   * Creates an empty aggregation buffer object. This is called before processing each key group
   * (group by key).
   *
   * @return an aggregation buffer object
   */
  override def createAggregationBuffer(): Array[Byte] = {
    if (useHeap) {
      val sketch = new Union(lgMaxK)
      sketch.toUpdatableByteArray
    } else {
      val bufferSize = HllSketch.getMaxUpdatableSerializationBytes(lgMaxK, TgtHllType.HLL_8)
      val buffer = new Array[Byte](bufferSize)
      new Union(lgMaxK, WritableMemory.writableWrap(buffer))
      buffer
    }
  }

  /**
   * Updates the aggregation buffer object with an input row and returns a new buffer object. For
   * performance, the function may do in-place update and return it instead of constructing new
   * buffer object.
   *
   * This is typically called when doing Partial or Complete mode aggregation.
   *
   * @param buffer The aggregation buffer object.
   * @param input  an input row
   */
  override def update(buffer: Array[Byte], input: InternalRow): Array[Byte] = {
    val union = byteArrayToUnion(buffer)
    val v = child.eval(input)
    if (v != null) {
      val inputSketch = HllSketch.wrap(WritableMemory.writableWrap(v.asInstanceOf[Array[Byte]]))
      union.update(inputSketch)
    }
    unionToByteArray(union, buffer)
  }

  /**
   * Merges an input aggregation object into aggregation buffer object and returns a new buffer
   * object. For performance, the function may do in-place merge and return it instead of
   * constructing new buffer object.
   *
   * This is typically called when doing PartialMerge or Final mode aggregation.
   *
   * @param buffer the aggregation buffer object used to store the aggregation result.
   * @param input  an input aggregation object. Input aggregation object can be produced by
   *               de-serializing the partial aggregate's output from Mapper side.
   */
  override def merge(buffer: Array[Byte], input: Array[Byte]): Array[Byte] = {
    val union = byteArrayToUnion(buffer)
    val sketch = HllSketch.wrap(WritableMemory.writableWrap(input))

    union.update(sketch)
    unionToByteArray(union, buffer)
  }

  /** Serializes the aggregation buffer object T to Array[Byte] */
  override def serialize(buffer: Array[Byte]): Array[Byte] = {
    buffer
  }

  /** De-serializes the serialized format Array[Byte], and produces aggregation buffer object T */
  override def deserialize(storageFormat: Array[Byte]): Array[Byte] = {
    storageFormat
  }

  /**
   * Generates the final aggregation result value for current key group with the aggregation buffer
   * object.
   *
   * Developer note: the only return types accepted by Spark are:
   *   - primitive types
   *   - InternalRow and subclasses
   *   - ArrayData
   *   - MapData
   *
   * @param buffer aggregation buffer object.
   * @return The aggregation result of current key group
   */
  override def eval(buffer: Array[Byte]): Any = {
    HllSketch.wrap(WritableMemory.writableWrap(buffer)).getEstimate.toLong
  }

  /**
   * Expected input types from child expressions. The i-th position in the returned seq indicates
   * the type requirement for the i-th child.
   *
   * The possible values at each position are:
   * 1. a specific data type, e.g. LongType, StringType.
   * 2. a non-leaf abstract data type, e.g. NumericType, IntegralType, FractionalType.
   */
  override def inputTypes: Seq[AbstractDataType] = Seq(BinaryType)
}