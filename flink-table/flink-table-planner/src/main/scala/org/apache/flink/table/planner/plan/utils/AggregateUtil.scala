/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.expressions._
import org.apache.flink.table.expressions.ExpressionUtils.extractValue
import org.apache.flink.table.functions._
import org.apache.flink.table.planner.JLong
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.functions.aggfunctions.{AvgAggFunction, CountAggFunction, Sum0AggFunction}
import org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction._
import org.apache.flink.table.planner.functions.aggfunctions.Sum0AggFunction._
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction
import org.apache.flink.table.planner.functions.inference.OperatorBindingCallContext
import org.apache.flink.table.planner.functions.sql.{FlinkSqlOperatorTable, SqlFirstLastValueAggFunction, SqlListAggFunction}
import org.apache.flink.table.planner.functions.utils.AggSqlFunction
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.planner.plan.`trait`.{ModifyKindSetTrait, ModifyKindSetTraitDef, RelModifiedMonotonicity}
import org.apache.flink.table.planner.plan.logical.{HoppingWindowSpec, WindowSpec}
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel
import org.apache.flink.table.planner.typeutils.LegacyDataViewUtils.useNullSerializerForStateViewFieldsFromAccType
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory
import org.apache.flink.table.runtime.dataview.{DataViewSpec, DataViewUtils}
import org.apache.flink.table.runtime.functions.aggregate.BuiltInAggregateFunction
import org.apache.flink.table.runtime.groupwindow._
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.TypeInferenceUtil
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks
import org.apache.flink.table.types.utils.DataTypeUtils

import org.apache.calcite.rel.`type`._
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.core.Aggregate.AggCallBinding
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.`type`.{SqlTypeName, SqlTypeUtil}
import org.apache.calcite.sql.{SqlAggFunction, SqlKind, SqlRankFunction}
import org.apache.calcite.sql.fun._
import org.apache.calcite.sql.validate.SqlMonotonicity

import java.time.Duration
import java.util

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AggregateUtil extends Enumeration {

  /**
   * Returns whether any of the aggregates are accurate DISTINCT.
   *
   * @return
   *   Whether any of the aggregates are accurate DISTINCT
   */
  def containsAccurateDistinctCall(aggCalls: util.List[AggregateCall]): Boolean = {
    aggCalls.exists(call => call.isDistinct && !call.isApproximate)
  }

  /**
   * Returns whether any of the aggregates are approximate DISTINCT.
   *
   * @return
   *   Whether any of the aggregates are approximate DISTINCT
   */
  def containsApproximateDistinctCall(aggCalls: util.List[AggregateCall]): Boolean = {
    aggCalls.exists(call => call.isDistinct && call.isApproximate)
  }

  /** Returns indices of group functions. */
  def getGroupIdExprIndexes(aggCalls: Seq[AggregateCall]): Seq[Int] = {
    aggCalls.zipWithIndex
      .filter {
        case (call, _) =>
          call.getAggregation.getKind match {
            case SqlKind.GROUP_ID | SqlKind.GROUPING | SqlKind.GROUPING_ID => true
            case _ => false
          }
      }
      .map { case (_, idx) => idx }
  }

  /**
   * Check whether AUXILIARY_GROUP aggCalls is in the front of the given agg's aggCallList, and
   * whether aggCallList contain AUXILIARY_GROUP when the given agg's groupSet is empty or the
   * indicator is true. Returns AUXILIARY_GROUP aggCalls' args and other aggCalls.
   *
   * @param agg
   *   aggregate
   * @return
   *   returns AUXILIARY_GROUP aggCalls' args and other aggCalls
   */
  def checkAndSplitAggCalls(agg: Aggregate): (Array[Int], Seq[AggregateCall]) = {
    var nonAuxGroupCallsStartIdx = -1

    val aggCalls = agg.getAggCallList
    aggCalls.zipWithIndex.foreach {
      case (call, idx) =>
        if (call.getAggregation == FlinkSqlOperatorTable.AUXILIARY_GROUP) {
          require(call.getArgList.size == 1)
        }
        if (nonAuxGroupCallsStartIdx >= 0) {
          // the left aggCalls should not be AUXILIARY_GROUP
          require(
            call.getAggregation != FlinkSqlOperatorTable.AUXILIARY_GROUP,
            "AUXILIARY_GROUP should be in the front of aggCall list")
        }
        if (
          nonAuxGroupCallsStartIdx < 0 &&
          call.getAggregation != FlinkSqlOperatorTable.AUXILIARY_GROUP
        ) {
          nonAuxGroupCallsStartIdx = idx
        }
    }

    if (nonAuxGroupCallsStartIdx < 0) {
      nonAuxGroupCallsStartIdx = aggCalls.length
    }

    val (auxGroupCalls, otherAggCalls) = aggCalls.splitAt(nonAuxGroupCallsStartIdx)
    if (agg.getGroupCount == 0) {
      require(
        auxGroupCalls.isEmpty,
        "AUXILIARY_GROUP aggCalls should be empty when groupSet is empty")
    }

    val auxGrouping = auxGroupCalls.map(_.getArgList.head.toInt).toArray
    require(auxGrouping.length + otherAggCalls.length == aggCalls.length)
    (auxGrouping, otherAggCalls)
  }

  def checkAndGetFullGroupSet(agg: Aggregate): Array[Int] = {
    val (auxGroupSet, _) = checkAndSplitAggCalls(agg)
    agg.getGroupSet.toArray ++ auxGroupSet
  }

  def getOutputIndexToAggCallIndexMap(
      typeFactory: FlinkTypeFactory,
      aggregateCalls: Seq[AggregateCall],
      inputType: RelDataType,
      isBounded: Boolean,
      orderKeyIndexes: Array[Int] = null): util.Map[Integer, Integer] = {
    val aggInfos = transformToAggregateInfoList(
      typeFactory,
      FlinkTypeFactory.toLogicalRowType(inputType),
      aggregateCalls,
      Array.fill(aggregateCalls.size)(false),
      orderKeyIndexes,
      needInputCount = false,
      Option.empty[Int],
      isStateBackedDataViews = false,
      needDistinctInfo = false,
      isBounded
    ).aggInfos

    val map = new util.HashMap[Integer, Integer]()
    var outputIndex = 0
    aggregateCalls.indices.foreach {
      aggCallIndex =>
        val aggInfo = aggInfos(aggCallIndex)
        val aggBuffers = aggInfo.externalAccTypes
        aggBuffers.indices.foreach {
          bufferIndex => map.put(outputIndex + bufferIndex, aggCallIndex)
        }
        outputIndex += aggBuffers.length
    }
    map
  }

  def createPartialAggInfoList(
      typeFactory: FlinkTypeFactory,
      partialLocalAggInputRowType: RowType,
      partialOriginalAggCalls: Seq[AggregateCall],
      partialAggCallNeedRetractions: Array[Boolean],
      partialAggNeedRetraction: Boolean,
      isGlobal: Boolean): AggregateInfoList = {
    transformToStreamAggregateInfoList(
      typeFactory,
      partialLocalAggInputRowType,
      partialOriginalAggCalls,
      partialAggCallNeedRetractions,
      partialAggNeedRetraction,
      isStateBackendDataViews = isGlobal
    )
  }

  def createIncrementalAggInfoList(
      typeFactory: FlinkTypeFactory,
      partialLocalAggInputRowType: RowType,
      partialOriginalAggCalls: Seq[AggregateCall],
      partialAggCallNeedRetractions: Array[Boolean],
      partialAggNeedRetraction: Boolean): AggregateInfoList = {
    val partialLocalAggInfoList = createPartialAggInfoList(
      typeFactory,
      partialLocalAggInputRowType,
      partialOriginalAggCalls,
      partialAggCallNeedRetractions,
      partialAggNeedRetraction,
      isGlobal = false)
    val partialGlobalAggInfoList = createPartialAggInfoList(
      typeFactory,
      partialLocalAggInputRowType,
      partialOriginalAggCalls,
      partialAggCallNeedRetractions,
      partialAggNeedRetraction,
      isGlobal = true)

    // pick distinct info from global which is on state, and modify excludeAcc parameter
    val incrementalDistinctInfos = partialGlobalAggInfoList.distinctInfos.map {
      info =>
        DistinctInfo(
          info.argIndexes,
          info.keyType,
          info.accType,
          // exclude distinct acc from the aggregate accumulator,
          // because the output acc only need to contain the count
          excludeAcc = true,
          info.dataViewSpec,
          info.consumeRetraction,
          info.filterArgs,
          info.aggIndexes
        )
    }

    AggregateInfoList(
      // pick local aggs info from local which is on heap
      partialLocalAggInfoList.aggInfos,
      partialGlobalAggInfoList.indexOfCountStar,
      partialGlobalAggInfoList.countStarInserted,
      incrementalDistinctInfos
    )
  }

  def deriveAggregateInfoList(
      agg: StreamPhysicalRel,
      groupCount: Int,
      aggCalls: Seq[AggregateCall]): AggregateInfoList = {
    val input = agg.getInput(0)
    val aggCallNeedRetractions = deriveAggCallNeedRetractions(agg, groupCount, aggCalls)
    val needInputCount = needRetraction(agg)
    transformToStreamAggregateInfoList(
      unwrapTypeFactory(agg),
      FlinkTypeFactory.toLogicalRowType(input.getRowType),
      aggCalls,
      aggCallNeedRetractions,
      needInputCount,
      isStateBackendDataViews = true
    )
  }

  def deriveStreamWindowAggregateInfoList(
      typeFactory: FlinkTypeFactory,
      inputRowType: RowType,
      aggCalls: Seq[AggregateCall],
      needRetraction: Boolean,
      windowSpec: WindowSpec,
      isStateBackendDataViews: Boolean): AggregateInfoList = {
    // Hopping window always requires additional COUNT(*) to determine whether to register next
    // timer through whether the current fired window is empty, see SliceSharedWindowAggProcessor.
    val needInputCount = windowSpec.isInstanceOf[HoppingWindowSpec] || needRetraction
    val aggSize = if (needInputCount) {
      // we may insert a count(*) when need input count
      aggCalls.length + 1
    } else {
      aggCalls.length
    }
    val aggCallNeedRetractions = Array.fill(aggSize)(needRetraction)
    transformToAggregateInfoList(
      typeFactory,
      inputRowType,
      aggCalls,
      aggCallNeedRetractions,
      orderKeyIndexes = null,
      needInputCount,
      Option.empty[Int],
      isStateBackendDataViews,
      needDistinctInfo = true,
      isBounded = false
    )
  }

  def deriveSumAndCountFromAvg(
      avgAggFunction: AvgAggFunction): (Sum0AggFunction, CountAggFunction) = {
    avgAggFunction match {
      case _: ByteAvgAggFunction => (new ByteSum0AggFunction, new CountAggFunction)
      case _: ShortAvgAggFunction => (new ShortSum0AggFunction, new CountAggFunction)
      case _: IntAvgAggFunction => (new IntSum0AggFunction, new CountAggFunction)
      case _: LongAvgAggFunction => (new LongSum0AggFunction, new CountAggFunction)
      case _: FloatAvgAggFunction => (new FloatSum0AggFunction, new CountAggFunction)
      case _: DoubleAvgAggFunction => (new DoubleSum0AggFunction, new CountAggFunction)
      case _ =>
        throw new TableException(
          s"Avg aggregate function does not support: ''$avgAggFunction''" +
            s"Please re-check the function or data type.")
    }
  }

  def transformToBatchAggregateFunctions(
      typeFactory: FlinkTypeFactory,
      inputRowType: RowType,
      aggregateCalls: Seq[AggregateCall],
      orderKeyIndexes: Array[Int] = null)
      : (Array[Array[Int]], Array[Array[DataType]], Array[UserDefinedFunction]) = {

    val aggInfos = transformToAggregateInfoList(
      typeFactory,
      inputRowType,
      aggregateCalls,
      Array.fill(aggregateCalls.size)(false),
      orderKeyIndexes,
      needInputCount = false,
      Option.empty[Int],
      isStateBackedDataViews = false,
      needDistinctInfo = false,
      isBounded = true
    ).aggInfos

    val aggFields = aggInfos.map(_.argIndexes)
    val bufferTypes = aggInfos.map(_.externalAccTypes)
    val functions = aggInfos.map(_.function)

    (aggFields, bufferTypes, functions)
  }

  def transformToBatchAggregateInfoList(
      typeFactory: FlinkTypeFactory,
      inputRowType: RowType,
      aggCalls: Seq[AggregateCall],
      aggCallNeedRetractions: Array[Boolean] = null,
      orderKeyIndexes: Array[Int] = null): AggregateInfoList = {

    val finalAggCallNeedRetractions = if (aggCallNeedRetractions == null) {
      Array.fill(aggCalls.size)(false)
    } else {
      aggCallNeedRetractions
    }

    transformToAggregateInfoList(
      typeFactory,
      inputRowType,
      aggCalls,
      finalAggCallNeedRetractions,
      orderKeyIndexes,
      needInputCount = false,
      Option.empty[Int],
      isStateBackedDataViews = false,
      needDistinctInfo = false,
      isBounded = true
    )
  }

  def transformToStreamAggregateInfoList(
      typeFactory: FlinkTypeFactory,
      inputRowType: RowType,
      aggregateCalls: Seq[AggregateCall],
      aggCallNeedRetractions: Array[Boolean],
      needInputCount: Boolean,
      isStateBackendDataViews: Boolean,
      needDistinctInfo: Boolean = true): AggregateInfoList = {
    transformToStreamAggregateInfoList(
      typeFactory,
      inputRowType,
      aggregateCalls,
      aggCallNeedRetractions,
      needInputCount,
      Option.empty[Int],
      isStateBackendDataViews,
      needDistinctInfo)
  }

  def transformToStreamAggregateInfoList(
      typeFactory: FlinkTypeFactory,
      inputRowType: RowType,
      aggregateCalls: Seq[AggregateCall],
      aggCallNeedRetractions: Array[Boolean],
      needInputCount: Boolean,
      indexOfExistingCountStar: Option[Int],
      isStateBackendDataViews: Boolean,
      needDistinctInfo: Boolean): AggregateInfoList = {
    transformToAggregateInfoList(
      typeFactory,
      inputRowType,
      aggregateCalls,
      aggCallNeedRetractions ++ Array(needInputCount), // for additional count(*)
      orderKeyIndexes = null,
      needInputCount,
      indexOfExistingCountStar,
      isStateBackendDataViews,
      needDistinctInfo,
      isBounded = false
    )
  }

  /**
   * Transforms calcite aggregate calls to AggregateInfos.
   *
   * @param inputRowType
   *   the input's output RowType
   * @param aggregateCalls
   *   the calcite aggregate calls
   * @param aggCallNeedRetractions
   *   whether the aggregate function need retract method
   * @param orderKeyIndexes
   *   the index of order by field in the input, null if not over agg
   * @param needInputCount
   *   whether need to calculate the input counts, which is used in aggregation with retraction
   *   input.If needed, insert a count(1) aggregate into the agg list.
   * @param indexOfExistingCountStar
   *   the index for the existing count star
   * @param isStateBackedDataViews
   *   whether the dataview in accumulator use state or heap
   * @param needDistinctInfo
   *   whether need to extract distinct information
   */
  private def transformToAggregateInfoList(
      typeFactory: FlinkTypeFactory,
      inputRowType: RowType,
      aggregateCalls: Seq[AggregateCall],
      aggCallNeedRetractions: Array[Boolean],
      orderKeyIndexes: Array[Int],
      needInputCount: Boolean,
      indexOfExistingCountStar: Option[Int],
      isStateBackedDataViews: Boolean,
      needDistinctInfo: Boolean,
      isBounded: Boolean): AggregateInfoList = {

    // Step-1:
    // if need inputCount, find count1 in the existed aggregate calls first,
    // if not exist, insert a new count1 and remember the index
    val (indexOfCountStar, countStarInserted, aggCalls) =
      insertCountStarAggCall(typeFactory, needInputCount, indexOfExistingCountStar, aggregateCalls)

    // Step-2:
    // extract distinct information from aggregate calls
    val (distinctInfos, newAggCalls) = extractDistinctInformation(
      needDistinctInfo,
      aggCalls,
      inputRowType,
      isStateBackedDataViews,
      needInputCount
    ) // needInputCount means whether the aggregate consume retractions

    // Step-3:
    // create aggregate information
    val factory =
      new AggFunctionFactory(inputRowType, orderKeyIndexes, aggCallNeedRetractions, isBounded)
    val aggInfos = newAggCalls.zipWithIndex
      .map {
        case (call, index) =>
          val argIndexes = call.getAggregation match {
            case _: SqlRankFunction =>
              if (orderKeyIndexes != null) orderKeyIndexes else Array[Int]()
            case _ => call.getArgList.map(_.intValue()).toArray
          }
          transformToAggregateInfo(
            inputRowType,
            call,
            index,
            argIndexes,
            factory.createAggFunction(call, index),
            isStateBackedDataViews,
            aggCallNeedRetractions(index))
      }

    AggregateInfoList(aggInfos.toArray, indexOfCountStar, countStarInserted, distinctInfos)
  }

  private def transformToAggregateInfo(
      inputRowType: RowType,
      call: AggregateCall,
      index: Int,
      argIndexes: Array[Int],
      udf: UserDefinedFunction,
      hasStateBackedDataViews: Boolean,
      needsRetraction: Boolean): AggregateInfo =
    call.getAggregation match {
      // In the new function stack, for imperativeFunction, the conversion from
      // BuiltInFunctionDefinition to SqlAggFunction is unnecessary, we can simply create
      // AggregateInfo through BuiltInFunctionDefinition and runtime implementation (obtained from
      // AggFunctionFactory) directly.
      // NOTE: make sure to use .runtimeProvided() in BuiltInFunctionDefinition in this case.
      case bridging: BridgingSqlAggFunction =>
        // The FunctionDefinition maybe also instance of DeclarativeAggregateFunction
        if (
          bridging.getDefinition.isInstanceOf[BuiltInFunctionDefinition] || bridging.getDefinition
            .isInstanceOf[DeclarativeAggregateFunction]
        ) {
          createAggregateInfoFromInternalFunction(
            call,
            udf,
            index,
            argIndexes,
            needsRetraction,
            hasStateBackedDataViews)
        } else {
          createAggregateInfoFromBridgingFunction(
            inputRowType,
            call,
            index,
            argIndexes,
            hasStateBackedDataViews,
            needsRetraction)
        }
      case _: AggSqlFunction =>
        createAggregateInfoFromLegacyFunction(
          inputRowType,
          call,
          index,
          argIndexes,
          udf.asInstanceOf[ImperativeAggregateFunction[_, _]],
          hasStateBackedDataViews,
          needsRetraction)

      case _: SqlAggFunction =>
        createAggregateInfoFromInternalFunction(
          call,
          udf,
          index,
          argIndexes,
          needsRetraction,
          hasStateBackedDataViews)
    }

  private def createAggregateInfoFromBridgingFunction(
      inputRowType: RowType,
      call: AggregateCall,
      index: Int,
      argIndexes: Array[Int],
      hasStateBackedDataViews: Boolean,
      needsRetraction: Boolean): AggregateInfo = {

    val function = call.getAggregation.asInstanceOf[BridgingSqlAggFunction]
    val definition = function.getDefinition
    val dataTypeFactory = function.getDataTypeFactory

    // not all information is available in the call context of aggregate functions at this location
    // e.g. literal information is lost because the aggregation is split into multiple operators
    val callContext = new OperatorBindingCallContext(
      dataTypeFactory,
      definition,
      new AggCallBinding(
        function.getTypeFactory,
        function,
        SqlTypeUtil.projectTypes(
          function.getTypeFactory.buildRelNodeRowType(inputRowType),
          argIndexes.map(Int.box).toList),
        0,
        false),
      call.getType)

    // create the final UDF for runtime
    val udf = UserDefinedFunctionHelper.createSpecializedFunction(
      function.getName,
      definition,
      callContext,
      classOf[PlannerBase].getClassLoader,
      // currently, aggregate functions have no access to FlinkContext
      null,
      null
    )
    val inference = udf.getTypeInference(dataTypeFactory)

    // enrich argument types with conversion class
    val castCallContext = TypeInferenceUtil.castArguments(inference, callContext, null)
    val enrichedArgumentDataTypes = toScala(castCallContext.getArgumentDataTypes)

    // derive accumulator type with conversion class
    val stateStrategies = inference.getStateTypeStrategies
    if (stateStrategies.size() != 1) {
      throw new ValidationException(
        "Aggregating functions must provide exactly one state type strategy.")
    }
    val accumulatorStrategy = stateStrategies.values().head
    val enrichedAccumulatorDataType =
      TypeInferenceUtil.inferOutputType(castCallContext, accumulatorStrategy)

    // enrich output types with conversion class
    val enrichedOutputDataType =
      TypeInferenceUtil.inferOutputType(castCallContext, inference.getOutputTypeStrategy)

    createImperativeAggregateInfo(
      call,
      udf.asInstanceOf[ImperativeAggregateFunction[_, _]],
      index,
      argIndexes,
      enrichedArgumentDataTypes.toArray,
      enrichedAccumulatorDataType,
      enrichedOutputDataType,
      needsRetraction,
      hasStateBackedDataViews
    )
  }

  private def createAggregateInfoFromInternalFunction(
      call: AggregateCall,
      udf: UserDefinedFunction,
      index: Int,
      argIndexes: Array[Int],
      needsRetraction: Boolean,
      hasStateBackedDataViews: Boolean): AggregateInfo = udf match {

    case imperativeFunction: BuiltInAggregateFunction[_, _] =>
      createImperativeAggregateInfo(
        call,
        imperativeFunction,
        index,
        argIndexes,
        imperativeFunction.getArgumentDataTypes.asScala.toArray,
        imperativeFunction.getAccumulatorDataType,
        imperativeFunction.getOutputDataType,
        needsRetraction,
        hasStateBackedDataViews
      )

    case declarativeFunction: DeclarativeAggregateFunction =>
      AggregateInfo(
        call,
        udf,
        index,
        argIndexes,
        null,
        declarativeFunction.getAggBufferTypes,
        Array(),
        declarativeFunction.getResultType,
        needsRetraction)
  }

  private def createImperativeAggregateInfo(
      call: AggregateCall,
      udf: ImperativeAggregateFunction[_, _],
      index: Int,
      argIndexes: Array[Int],
      inputDataTypes: Array[DataType],
      accumulatorDataType: DataType,
      outputDataType: DataType,
      needsRetraction: Boolean,
      hasStateBackedDataViews: Boolean): AggregateInfo = {

    // extract data views and adapt the data views in the accumulator type
    // if a view is backed by a state backend
    val dataViewSpecs: Array[DataViewSpec] = if (hasStateBackedDataViews) {
      DataViewUtils.extractDataViews(index, accumulatorDataType).asScala.toArray
    } else {
      Array()
    }
    val adjustedAccumulatorDataType =
      DataViewUtils.adjustDataViews(accumulatorDataType, hasStateBackedDataViews)

    AggregateInfo(
      call,
      udf,
      index,
      argIndexes,
      inputDataTypes,
      Array(adjustedAccumulatorDataType),
      dataViewSpecs,
      outputDataType,
      needsRetraction)
  }

  private def createAggregateInfoFromLegacyFunction(
      inputRowType: RowType,
      call: AggregateCall,
      index: Int,
      argIndexes: Array[Int],
      udf: UserDefinedFunction,
      hasStateBackedDataViews: Boolean,
      needsRetraction: Boolean): AggregateInfo = {
    val (externalArgTypes, externalAccTypes, viewSpecs, externalResultType) = udf match {
      case a: ImperativeAggregateFunction[_, _] =>
        val (implicitAccType, implicitResultType) = call.getAggregation match {
          case aggSqlFun: AggSqlFunction =>
            (aggSqlFun.externalAccType, aggSqlFun.externalResultType)
          case _ => (null, null)
        }
        val externalAccType = getAccumulatorTypeOfAggregateFunction(a, implicitAccType)
        val argTypes = call.getArgList
          .map(idx => inputRowType.getChildren.get(idx))
        val externalArgTypes: Array[DataType] =
          getAggUserDefinedInputTypes(a, externalAccType, argTypes.toArray)
        val (newExternalAccType, specs) = useNullSerializerForStateViewFieldsFromAccType(
          index,
          a,
          externalAccType,
          hasStateBackedDataViews)
        (
          externalArgTypes,
          Array(newExternalAccType),
          specs,
          getResultTypeOfAggregateFunction(a, implicitResultType)
        )

      case _ => throw new TableException(s"Unsupported function: $udf")
    }

    AggregateInfo(
      call,
      udf,
      index,
      argIndexes,
      externalArgTypes,
      externalAccTypes,
      viewSpecs,
      externalResultType,
      needsRetraction)
  }

  /**
   * Inserts an COUNT(*) aggregate call if needed. The COUNT(*) aggregate call is used to count the
   * number of added and retracted input records.
   *
   * @param needInputCount
   *   whether to insert an InputCount aggregate
   * @param aggregateCalls
   *   original aggregate calls
   * @param indexOfExistingCountStar
   *   the index for the existing count star
   * @return
   *   (indexOfCountStar, countStarInserted, newAggCalls)
   */
  private def insertCountStarAggCall(
      typeFactory: FlinkTypeFactory,
      needInputCount: Boolean,
      indexOfExistingCountStar: Option[Int],
      aggregateCalls: Seq[AggregateCall]): (Option[Int], Boolean, Seq[AggregateCall]) = {

    if (indexOfExistingCountStar.getOrElse(-1) >= 0) {
      require(needInputCount)
      return (indexOfExistingCountStar, false, aggregateCalls)
    }

    var indexOfCountStar: Option[Int] = None
    var countStarInserted: Boolean = false
    if (!needInputCount) {
      return (indexOfCountStar, countStarInserted, aggregateCalls)
    }

    // if need inputCount, find count(*) in the existed aggregate calls first,
    // if not exist, insert a new count(*) and remember the index
    var newAggCalls = aggregateCalls
    aggregateCalls.zipWithIndex.foreach {
      case (call, index) =>
        if (
          call.getAggregation.isInstanceOf[SqlCountAggFunction] &&
          call.filterArg < 0 &&
          call.getArgList.isEmpty &&
          !call.isApproximate &&
          !call.isDistinct
        ) {
          indexOfCountStar = Some(index)
        }
    }

    // count(*) not exist in aggregateCalls, insert a count(*) in it.
    if (indexOfCountStar.isEmpty) {
      val count1 = AggregateCall.create(
        SqlStdOperatorTable.COUNT,
        false,
        false,
        false,
        new util.ArrayList[RexNode](),
        new util.ArrayList[Integer](),
        -1,
        null,
        RelCollations.EMPTY,
        typeFactory.createSqlType(SqlTypeName.BIGINT),
        "_$count1$_"
      )

      indexOfCountStar = Some(aggregateCalls.length)
      countStarInserted = true
      newAggCalls = aggregateCalls ++ Seq(count1)
    }

    (indexOfCountStar, countStarInserted, newAggCalls)
  }

  /**
   * Extracts DistinctInfo array from the aggregate calls, and change the distinct aggregate to
   * non-distinct aggregate.
   *
   * @param needDistinctInfo
   *   whether to extract distinct information
   * @param aggCalls
   *   the original aggregate calls
   * @param inputType
   *   the input rel data type
   * @param hasStateBackedDataViews
   *   whether the dataview in accumulator use state or heap
   * @param consumeRetraction
   *   whether the distinct aggregate consumes retraction messages
   * @return
   *   (distinctInfoArray, newAggCalls)
   */
  private def extractDistinctInformation(
      needDistinctInfo: Boolean,
      aggCalls: Seq[AggregateCall],
      inputType: RowType,
      hasStateBackedDataViews: Boolean,
      consumeRetraction: Boolean): (Array[DistinctInfo], Seq[AggregateCall]) = {

    if (!needDistinctInfo) {
      return (Array(), aggCalls)
    }

    val distinctMap = mutable.LinkedHashMap.empty[String, DistinctInfo]
    val newAggCalls = aggCalls.zipWithIndex.map {
      case (call, index) =>
        val argIndexes = call.getArgList.map(_.intValue()).toArray

        // extract distinct information and replace a new call
        if (call.isDistinct && !call.isApproximate && argIndexes.length > 0) {
          val argTypes: Array[LogicalType] = call.getArgList
            .map(inputType.getChildren.get(_))
            .toArray

          val keyType = createDistinctKeyType(argTypes)
          val keyDataType = DataTypeUtils.toInternalDataType(keyType)
          val distinctInfo = distinctMap.getOrElseUpdate(
            argIndexes.mkString(","),
            DistinctInfo(
              argIndexes,
              keyDataType,
              null, // later fill in
              excludeAcc = false,
              null, // later fill in
              consumeRetraction,
              ArrayBuffer.empty[Int],
              ArrayBuffer.empty[Int])
          )
          // add current agg to the distinct agg list
          distinctInfo.filterArgs += call.filterArg
          distinctInfo.aggIndexes += index

          AggregateCall.create(
            call.getAggregation,
            false,
            false,
            call.ignoreNulls,
            call.rexList,
            call.getArgList,
            -1, // remove filterArg
            null,
            RelCollations.EMPTY,
            call.getType,
            call.getName
          )
        } else {
          call
        }
    }

    // fill in the acc type and data view spec
    val filterArgsLimit = if (consumeRetraction) {
      1
    } else {
      64
    }
    val distinctInfos = distinctMap.values.zipWithIndex.map {
      case (d, index) =>
        val distinctViewDataType =
          DataViewUtils.createDistinctViewDataType(d.keyType, d.filterArgs.length, filterArgsLimit)

        // create data views and adapt the data views in the accumulator type
        // if a view is backed by a state backend
        val distinctViewSpec = if (hasStateBackedDataViews) {
          Some(DataViewUtils.createDistinctViewSpec(index, distinctViewDataType))
        } else {
          None
        }
        val adjustedAccumulatorDataType =
          DataViewUtils.adjustDataViews(distinctViewDataType, hasStateBackedDataViews)

        DistinctInfo(
          d.argIndexes,
          d.keyType,
          adjustedAccumulatorDataType,
          excludeAcc = false,
          distinctViewSpec,
          consumeRetraction,
          d.filterArgs,
          d.aggIndexes)
    }

    (distinctInfos.toArray, newAggCalls)
  }

  def createDistinctKeyType(argTypes: Array[LogicalType]): LogicalType = {
    if (argTypes.length == 1) {
      argTypes(0).getTypeRoot match {
        // ordered by type root definition
        case CHAR | VARCHAR | BOOLEAN | DECIMAL | TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT |
            DOUBLE | DATE | TIME_WITHOUT_TIME_ZONE | TIMESTAMP_WITHOUT_TIME_ZONE |
            TIMESTAMP_WITH_LOCAL_TIME_ZONE | INTERVAL_YEAR_MONTH | INTERVAL_DAY_TIME | ARRAY |
            VARIANT =>
          argTypes(0)
        case t =>
          throw new TableException(
            s"Distinct aggregate function does not support type: $t.\n" +
              s"Please re-check the data type.")
      }
    } else {
      RowType.of(argTypes: _*)
    }
  }

  /** Return true if all aggregates can be partially merged. False otherwise. */
  def doAllSupportPartialMerge(aggInfos: Array[AggregateInfo]): Boolean = {
    val supportMerge = aggInfos.map(_.function).forall {
      case _: DeclarativeAggregateFunction => true
      case a => ifMethodExistInFunction("merge", a)
    }

    // it means grouping without aggregate functions
    aggInfos.isEmpty || supportMerge
  }

  /**
   * Return true if all aggregates can be projected for adaptive local hash aggregate. False
   * otherwise.
   */
  def doAllAggSupportAdaptiveLocalHashAgg(aggCalls: Seq[AggregateCall]): Boolean = {
    aggCalls.forall {
      aggCall =>
        // TODO support adaptive local hash agg while agg call with filter condition.
        if (aggCall.filterArg >= 0) {
          return false
        }
        aggCall.getAggregation match {
          case _: SqlCountAggFunction | _: SqlAvgAggFunction | _: SqlMinMaxAggFunction |
              _: SqlSumAggFunction =>
            true
          case _ => false
        }
    }
  }

  /** Return true if all aggregates can be split. False otherwise. */
  def doAllAggSupportSplit(aggCalls: util.List[AggregateCall]): Boolean = {
    aggCalls.forall {
      aggCall =>
        aggCall.getAggregation match {
          case _: SqlCountAggFunction | _: SqlAvgAggFunction | _: SqlMinMaxAggFunction |
              _: SqlSumAggFunction | _: SqlSumEmptyIsZeroAggFunction |
              _: SqlSingleValueAggFunction =>
            true
          case _: SqlFirstLastValueAggFunction | _: SqlListAggFunction =>
            aggCall.getArgList.size() == 1
          case _ => false
        }
    }
  }

  /** Derives output row type from stream local aggregate */
  def inferStreamLocalAggRowType(
      aggInfoList: AggregateInfoList,
      inputType: RelDataType,
      groupSet: Array[Int],
      typeFactory: FlinkTypeFactory): RelDataType = {
    val accTypes = aggInfoList.getAccTypes
    val groupingTypes = groupSet
      .map(inputType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toLogicalType)
    val groupingNames = groupSet.map(inputType.getFieldNames.get(_))
    val accFieldNames = inferStreamAggAccumulatorNames(aggInfoList)

    typeFactory.buildRelNodeRowType(
      groupingNames ++ accFieldNames,
      groupingTypes ++ accTypes.map(fromDataTypeToLogicalType))
  }

  /** Derives accumulators names from stream aggregate */
  def inferStreamAggAccumulatorNames(aggInfoList: AggregateInfoList): Array[String] = {
    var index = -1
    val aggBufferNames = aggInfoList.aggInfos.indices.flatMap {
      i =>
        aggInfoList.aggInfos(i).function match {
          case _: AggregateFunction[_, _] =>
            val name = aggInfoList.aggInfos(i).agg.getAggregation.getName.toLowerCase
            index += 1
            Array(s"$name$$$index")
          case daf: DeclarativeAggregateFunction =>
            daf.aggBufferAttributes.map {
              a =>
                index += 1
                s"${a.getName}$$$index"
            }
        }
    }
    val distinctBufferNames = aggInfoList.distinctInfos.indices.map(i => s"distinct$$$i")
    (aggBufferNames ++ distinctBufferNames).toArray
  }

  /** Return true if the given agg rel needs retraction message, else false. */
  def needRetraction(agg: StreamPhysicalRel): Boolean = {
    // need to call `retract()` if input contains update or delete
    val modifyKindSetTrait = agg.getInput(0).getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
    if (modifyKindSetTrait == null || modifyKindSetTrait == ModifyKindSetTrait.EMPTY) {
      // FlinkChangelogModeInferenceProgram is not applied yet, false as default
      false
    } else {
      !modifyKindSetTrait.modifyKindSet.isInsertOnly
    }
  }

  /**
   * Return the retraction flags for each given agg calls, currently MAX and MIN are supported.
   * MaxWithRetract can be optimized to Max if input is update increasing, MinWithRetract can be
   * optimized to Min if input is update decreasing.
   */
  def deriveAggCallNeedRetractions(
      agg: StreamPhysicalRel,
      groupCount: Int,
      aggCalls: Seq[AggregateCall]): Array[Boolean] = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(agg.getCluster.getMetadataQuery)
    val monotonicity = fmq.getRelModifiedMonotonicity(agg)
    val needRetractionFlag = needRetraction(agg)
    deriveAggCallNeedRetractions(groupCount, aggCalls, needRetractionFlag, monotonicity)
  }

  /**
   * Return the retraction flags for each given agg calls, currently max and min are supported.
   * MaxWithRetract can be optimized to Max if input is update increasing, MinWithRetract can be
   * optimized to Min if input is update decreasing.
   */
  def deriveAggCallNeedRetractions(
      groupCount: Int,
      aggCalls: Seq[AggregateCall],
      needRetraction: Boolean,
      monotonicity: RelModifiedMonotonicity): Array[Boolean] = {
    val needRetractionArray = Array.fill(aggCalls.size)(needRetraction)
    if (monotonicity != null && needRetraction) {
      aggCalls.zipWithIndex.foreach {
        case (aggCall, idx) =>
          aggCall.getAggregation match {
            // if monotonicity is decreasing and aggCall is min with retract,
            // set needRetraction to false
            case a: SqlMinMaxAggFunction
                if a.getKind == SqlKind.MIN &&
                  monotonicity.fieldMonotonicities(
                    groupCount + idx) == SqlMonotonicity.DECREASING =>
              needRetractionArray(idx) = false
            // if monotonicity is increasing and aggCall is max with retract,
            // set needRetraction to false
            case a: SqlMinMaxAggFunction
                if a.getKind == SqlKind.MAX &&
                  monotonicity.fieldMonotonicities(
                    groupCount + idx) == SqlMonotonicity.INCREASING =>
              needRetractionArray(idx) = false
            case _ => // do nothing
          }
      }
    }

    needRetractionArray
  }

  /** Derives output row type from local aggregate */
  def inferLocalAggRowType(
      aggInfoList: AggregateInfoList,
      inputRowType: RelDataType,
      groupSet: Array[Int],
      typeFactory: FlinkTypeFactory): RelDataType = {
    val accTypes = aggInfoList.getAccTypes
    val groupingTypes = groupSet
      .map(inputRowType.getFieldList.get(_).getType)
      .map(FlinkTypeFactory.toLogicalType)
    val groupingNames = groupSet.map(inputRowType.getFieldNames.get(_))
    val accFieldNames = inferAggAccumulatorNames(aggInfoList)

    typeFactory.buildRelNodeRowType(
      groupingNames ++ accFieldNames,
      groupingTypes ++ accTypes.map(fromDataTypeToLogicalType))
  }

  /** Derives accumulators names from aggregate */
  def inferAggAccumulatorNames(aggInfoList: AggregateInfoList): Array[String] = {
    var index = -1
    val aggBufferNames = aggInfoList.aggInfos.indices.flatMap {
      i =>
        aggInfoList.aggInfos(i).function match {
          case _: AggregateFunction[_, _] =>
            val name = aggInfoList.aggInfos(i).agg.getAggregation.getName.toLowerCase
            index += 1
            Array(s"$name$$$index")
          case daf: DeclarativeAggregateFunction =>
            daf.aggBufferAttributes.map {
              a =>
                index += 1
                s"${a.getName}$$$index"
            }
        }
    }
    val distinctBufferNames = aggInfoList.distinctInfos.indices.map(i => s"distinct$$$i")
    (aggBufferNames ++ distinctBufferNames).toArray
  }

  /** Computes the positions of (window start, window end, row time). */
  private[flink] def computeWindowPropertyPos(
      properties: Seq[NamedWindowProperty]): (Option[Int], Option[Int], Option[Int]) = {
    val propPos =
      properties.foldRight((None: Option[Int], None: Option[Int], None: Option[Int], 0)) {
        case (p, (s, e, rt, i)) =>
          p match {
            case p: NamedWindowProperty =>
              p.getProperty match {
                case _: WindowStart if s.isDefined =>
                  throw new TableException(
                    "Duplicate window start property encountered. This is a bug.")
                case _: WindowStart =>
                  (Some(i), e, rt, i - 1)
                case _: WindowEnd if e.isDefined =>
                  throw new TableException(
                    "Duplicate window end property encountered. This is a bug.")
                case _: WindowEnd =>
                  (s, Some(i), rt, i - 1)
                case _: RowtimeAttribute if rt.isDefined =>
                  throw new TableException(
                    "Duplicate window rowtime property encountered. This is a bug.")
                case _: RowtimeAttribute =>
                  (s, e, Some(i), i - 1)
                case _: ProctimeAttribute =>
                  // ignore this property, it will be null at the position later
                  (s, e, rt, i - 1)
              }
          }
      }
    (propPos._1, propPos._2, propPos._3)
  }

  def isRowtimeAttribute(field: FieldReferenceExpression): Boolean = {
    LogicalTypeChecks.isRowtimeAttribute(field.getOutputDataType.getLogicalType)
  }

  def isProctimeAttribute(field: FieldReferenceExpression): Boolean = {
    LogicalTypeChecks.isProctimeAttribute(field.getOutputDataType.getLogicalType)
  }

  def hasTimeIntervalType(intervalType: ValueLiteralExpression): Boolean = {
    intervalType.getOutputDataType.getLogicalType.is(LogicalTypeRoot.INTERVAL_DAY_TIME)
  }

  def hasRowIntervalType(intervalType: ValueLiteralExpression): Boolean = {
    intervalType.getOutputDataType.getLogicalType.is(LogicalTypeRoot.BIGINT)
  }

  def toLong(literalExpr: ValueLiteralExpression): JLong =
    extractValue(literalExpr, classOf[JLong]).get()

  def toDuration(literalExpr: ValueLiteralExpression): Duration =
    extractValue(literalExpr, classOf[Duration]).get()

  def isTableAggregate(aggCalls: util.List[AggregateCall]): Boolean = {
    aggCalls
      .flatMap(
        call =>
          call.getAggregation match {
            case asf: AggSqlFunction => Some(asf.aggregateFunction)
            case bsaf: BridgingSqlAggFunction => Some(bsaf.getDefinition)
            case _ => None
          })
      .exists(_.getKind == FunctionKind.TABLE_AGGREGATE)
  }

  def isAsyncStateEnabled(config: ReadableConfig, aggInfoList: AggregateInfoList): Boolean = {
    // Currently, we do not support async state with agg functions that include DataView.
    val containsDataViewInAggInfo =
      aggInfoList.aggInfos.toStream.stream().anyMatch(agg => !agg.viewSpecs.isEmpty)

    val containsDataViewInDistinctInfo =
      aggInfoList.distinctInfos.toStream
        .stream()
        .anyMatch(distinct => distinct.dataViewSpec.isDefined)

    config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED) &&
    !containsDataViewInAggInfo &&
    !containsDataViewInDistinctInfo
  }
}
