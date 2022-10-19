/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.io;

import java.io.IOException;
import java.util.List;
import org.apache.solr.io.comp.ComparatorOrder;
import org.apache.solr.io.comp.FieldComparator;
import org.apache.solr.io.comp.MultipleFieldComparator;
import org.apache.solr.io.comp.StreamComparator;
import org.apache.solr.io.eval.AbsoluteValueEvaluator;
import org.apache.solr.io.eval.AddEvaluator;
import org.apache.solr.io.eval.AkimaEvaluator;
import org.apache.solr.io.eval.AndEvaluator;
import org.apache.solr.io.eval.AnovaEvaluator;
import org.apache.solr.io.eval.AppendEvaluator;
import org.apache.solr.io.eval.ArcCosineEvaluator;
import org.apache.solr.io.eval.ArcSineEvaluator;
import org.apache.solr.io.eval.ArcTangentEvaluator;
import org.apache.solr.io.eval.ArrayEvaluator;
import org.apache.solr.io.eval.AscEvaluator;
import org.apache.solr.io.eval.BetaDistributionEvaluator;
import org.apache.solr.io.eval.BicubicSplineEvaluator;
import org.apache.solr.io.eval.BinomialCoefficientEvaluator;
import org.apache.solr.io.eval.BinomialDistributionEvaluator;
import org.apache.solr.io.eval.CanberraEvaluator;
import org.apache.solr.io.eval.CeilingEvaluator;
import org.apache.solr.io.eval.ChebyshevEvaluator;
import org.apache.solr.io.eval.ChiSquareDataSetEvaluator;
import org.apache.solr.io.eval.CoalesceEvaluator;
import org.apache.solr.io.eval.ColumnAtEvaluator;
import org.apache.solr.io.eval.ColumnCountEvaluator;
import org.apache.solr.io.eval.ColumnEvaluator;
import org.apache.solr.io.eval.ConcatEvaluator;
import org.apache.solr.io.eval.ConstantDistributionEvaluator;
import org.apache.solr.io.eval.ConversionEvaluator;
import org.apache.solr.io.eval.ConvexHullEvaluator;
import org.apache.solr.io.eval.ConvolutionEvaluator;
import org.apache.solr.io.eval.CopyOfEvaluator;
import org.apache.solr.io.eval.CopyOfRangeEvaluator;
import org.apache.solr.io.eval.CorrelationEvaluator;
import org.apache.solr.io.eval.CorrelationSignificanceEvaluator;
import org.apache.solr.io.eval.CosineDistanceEvaluator;
import org.apache.solr.io.eval.CosineEvaluator;
import org.apache.solr.io.eval.CosineSimilarityEvaluator;
import org.apache.solr.io.eval.CovarianceEvaluator;
import org.apache.solr.io.eval.CubedRootEvaluator;
import org.apache.solr.io.eval.CumulativeProbabilityEvaluator;
import org.apache.solr.io.eval.DateEvaluator;
import org.apache.solr.io.eval.DbscanEvaluator;
import org.apache.solr.io.eval.DensityEvaluator;
import org.apache.solr.io.eval.DerivativeEvaluator;
import org.apache.solr.io.eval.DescribeEvaluator;
import org.apache.solr.io.eval.DistanceEvaluator;
import org.apache.solr.io.eval.DivideEvaluator;
import org.apache.solr.io.eval.DotProductEvaluator;
import org.apache.solr.io.eval.DoubleEvaluator;
import org.apache.solr.io.eval.EBEAddEvaluator;
import org.apache.solr.io.eval.EBEDivideEvaluator;
import org.apache.solr.io.eval.EBEMultiplyEvaluator;
import org.apache.solr.io.eval.EBESubtractEvaluator;
import org.apache.solr.io.eval.EarthMoversEvaluator;
import org.apache.solr.io.eval.EmpiricalDistributionEvaluator;
import org.apache.solr.io.eval.EnclosingDiskEvaluator;
import org.apache.solr.io.eval.EnumeratedDistributionEvaluator;
import org.apache.solr.io.eval.EqualToEvaluator;
import org.apache.solr.io.eval.EuclideanEvaluator;
import org.apache.solr.io.eval.ExclusiveOrEvaluator;
import org.apache.solr.io.eval.ExponentialMovingAverageEvaluator;
import org.apache.solr.io.eval.FFTEvaluator;
import org.apache.solr.io.eval.FactorialEvaluator;
import org.apache.solr.io.eval.FeatureSelectEvaluator;
import org.apache.solr.io.eval.FindDelayEvaluator;
import org.apache.solr.io.eval.FloorEvaluator;
import org.apache.solr.io.eval.FrequencyTableEvaluator;
import org.apache.solr.io.eval.FuzzyKmeansEvaluator;
import org.apache.solr.io.eval.GTestDataSetEvaluator;
import org.apache.solr.io.eval.GammaDistributionEvaluator;
import org.apache.solr.io.eval.GaussFitEvaluator;
import org.apache.solr.io.eval.GeometricDistributionEvaluator;
import org.apache.solr.io.eval.GetAmplitudeEvaluator;
import org.apache.solr.io.eval.GetAngularFrequencyEvaluator;
import org.apache.solr.io.eval.GetAreaEvaluator;
import org.apache.solr.io.eval.GetAttributeEvaluator;
import org.apache.solr.io.eval.GetAttributesEvaluator;
import org.apache.solr.io.eval.GetBaryCenterEvaluator;
import org.apache.solr.io.eval.GetBoundarySizeEvaluator;
import org.apache.solr.io.eval.GetCacheEvaluator;
import org.apache.solr.io.eval.GetCenterEvaluator;
import org.apache.solr.io.eval.GetCentroidsEvaluator;
import org.apache.solr.io.eval.GetClusterEvaluator;
import org.apache.solr.io.eval.GetColumnLabelsEvaluator;
import org.apache.solr.io.eval.GetMembershipMatrixEvaluator;
import org.apache.solr.io.eval.GetPhaseEvaluator;
import org.apache.solr.io.eval.GetRadiusEvaluator;
import org.apache.solr.io.eval.GetRowLabelsEvaluator;
import org.apache.solr.io.eval.GetSupportPointsEvaluator;
import org.apache.solr.io.eval.GetValueEvaluator;
import org.apache.solr.io.eval.GetVerticesEvaluator;
import org.apache.solr.io.eval.GrandSumEvaluator;
import org.apache.solr.io.eval.GreaterThanEqualToEvaluator;
import org.apache.solr.io.eval.GreaterThanEvaluator;
import org.apache.solr.io.eval.HarmonicFitEvaluator;
import org.apache.solr.io.eval.HistogramEvaluator;
import org.apache.solr.io.eval.HyperbolicCosineEvaluator;
import org.apache.solr.io.eval.HyperbolicSineEvaluator;
import org.apache.solr.io.eval.HyperbolicTangentEvaluator;
import org.apache.solr.io.eval.IFFTEvaluator;
import org.apache.solr.io.eval.IfThenElseEvaluator;
import org.apache.solr.io.eval.IndexOfEvaluator;
import org.apache.solr.io.eval.IntegrateEvaluator;
import org.apache.solr.io.eval.IsNullEvaluator;
import org.apache.solr.io.eval.KmeansEvaluator;
import org.apache.solr.io.eval.KnnEvaluator;
import org.apache.solr.io.eval.KnnRegressionEvaluator;
import org.apache.solr.io.eval.KolmogorovSmirnovEvaluator;
import org.apache.solr.io.eval.L1NormEvaluator;
import org.apache.solr.io.eval.LInfNormEvaluator;
import org.apache.solr.io.eval.LatLonVectorsEvaluator;
import org.apache.solr.io.eval.LeftShiftEvaluator;
import org.apache.solr.io.eval.LengthEvaluator;
import org.apache.solr.io.eval.LerpEvaluator;
import org.apache.solr.io.eval.LessThanEqualToEvaluator;
import org.apache.solr.io.eval.LessThanEvaluator;
import org.apache.solr.io.eval.ListCacheEvaluator;
import org.apache.solr.io.eval.LoessEvaluator;
import org.apache.solr.io.eval.Log10Evaluator;
import org.apache.solr.io.eval.LogNormalDistributionEvaluator;
import org.apache.solr.io.eval.LongEvaluator;
import org.apache.solr.io.eval.LowerEvaluator;
import org.apache.solr.io.eval.ManhattanEvaluator;
import org.apache.solr.io.eval.MannWhitneyUEvaluator;
import org.apache.solr.io.eval.MarkovChainEvaluator;
import org.apache.solr.io.eval.MatchesEvaluator;
import org.apache.solr.io.eval.MatrixEvaluator;
import org.apache.solr.io.eval.MatrixMultiplyEvaluator;
import org.apache.solr.io.eval.MeanDifferenceEvaluator;
import org.apache.solr.io.eval.MeanEvaluator;
import org.apache.solr.io.eval.MemsetEvaluator;
import org.apache.solr.io.eval.MinMaxScaleEvaluator;
import org.apache.solr.io.eval.ModeEvaluator;
import org.apache.solr.io.eval.ModuloEvaluator;
import org.apache.solr.io.eval.MonteCarloEvaluator;
import org.apache.solr.io.eval.MovingAverageEvaluator;
import org.apache.solr.io.eval.MovingMADEvaluator;
import org.apache.solr.io.eval.MovingMedianEvaluator;
import org.apache.solr.io.eval.MultiKmeansEvaluator;
import org.apache.solr.io.eval.MultiVariateNormalDistributionEvaluator;
import org.apache.solr.io.eval.MultiplyEvaluator;
import org.apache.solr.io.eval.NaturalEvaluator;
import org.apache.solr.io.eval.NaturalLogEvaluator;
import org.apache.solr.io.eval.NormEvaluator;
import org.apache.solr.io.eval.NormalDistributionEvaluator;
import org.apache.solr.io.eval.NormalizeEvaluator;
import org.apache.solr.io.eval.NormalizeSumEvaluator;
import org.apache.solr.io.eval.NotEvaluator;
import org.apache.solr.io.eval.NotNullEvaluator;
import org.apache.solr.io.eval.OLSRegressionEvaluator;
import org.apache.solr.io.eval.OnesEvaluator;
import org.apache.solr.io.eval.OrEvaluator;
import org.apache.solr.io.eval.OscillateEvaluator;
import org.apache.solr.io.eval.OutliersEvaluator;
import org.apache.solr.io.eval.PairSortEvaluator;
import org.apache.solr.io.eval.PairedTTestEvaluator;
import org.apache.solr.io.eval.PercentileEvaluator;
import org.apache.solr.io.eval.PivotEvaluator;
import org.apache.solr.io.eval.PoissonDistributionEvaluator;
import org.apache.solr.io.eval.PolyFitEvaluator;
import org.apache.solr.io.eval.PowerEvaluator;
import org.apache.solr.io.eval.PrecisionEvaluator;
import org.apache.solr.io.eval.PredictEvaluator;
import org.apache.solr.io.eval.PrimesEvaluator;
import org.apache.solr.io.eval.ProbabilityEvaluator;
import org.apache.solr.io.eval.ProjectToBorderEvaluator;
import org.apache.solr.io.eval.PutCacheEvaluator;
import org.apache.solr.io.eval.RankEvaluator;
import org.apache.solr.io.eval.RawValueEvaluator;
import org.apache.solr.io.eval.RecNumEvaluator;
import org.apache.solr.io.eval.RecipEvaluator;
import org.apache.solr.io.eval.RegressionEvaluator;
import org.apache.solr.io.eval.RemoveCacheEvaluator;
import org.apache.solr.io.eval.RepeatEvaluator;
import org.apache.solr.io.eval.ReverseEvaluator;
import org.apache.solr.io.eval.RightShiftEvaluator;
import org.apache.solr.io.eval.RoundEvaluator;
import org.apache.solr.io.eval.RowAtEvaluator;
import org.apache.solr.io.eval.RowCountEvaluator;
import org.apache.solr.io.eval.SampleEvaluator;
import org.apache.solr.io.eval.ScalarAddEvaluator;
import org.apache.solr.io.eval.ScalarDivideEvaluator;
import org.apache.solr.io.eval.ScalarMultiplyEvaluator;
import org.apache.solr.io.eval.ScalarSubtractEvaluator;
import org.apache.solr.io.eval.ScaleEvaluator;
import org.apache.solr.io.eval.SequenceEvaluator;
import org.apache.solr.io.eval.SetColumnLabelsEvaluator;
import org.apache.solr.io.eval.SetRowLabelsEvaluator;
import org.apache.solr.io.eval.SetValueEvaluator;
import org.apache.solr.io.eval.SineEvaluator;
import org.apache.solr.io.eval.SplineEvaluator;
import org.apache.solr.io.eval.SplitEvaluator;
import org.apache.solr.io.eval.SquareRootEvaluator;
import org.apache.solr.io.eval.StandardDeviationEvaluator;
import org.apache.solr.io.eval.SubtractEvaluator;
import org.apache.solr.io.eval.SumColumnsEvaluator;
import org.apache.solr.io.eval.SumDifferenceEvaluator;
import org.apache.solr.io.eval.SumRowsEvaluator;
import org.apache.solr.io.eval.SumSqEvaluator;
import org.apache.solr.io.eval.TTestEvaluator;
import org.apache.solr.io.eval.TangentEvaluator;
import org.apache.solr.io.eval.TemporalEvaluatorDay;
import org.apache.solr.io.eval.TemporalEvaluatorDayOfQuarter;
import org.apache.solr.io.eval.TemporalEvaluatorDayOfYear;
import org.apache.solr.io.eval.TemporalEvaluatorEpoch;
import org.apache.solr.io.eval.TemporalEvaluatorHour;
import org.apache.solr.io.eval.TemporalEvaluatorMinute;
import org.apache.solr.io.eval.TemporalEvaluatorMonth;
import org.apache.solr.io.eval.TemporalEvaluatorQuarter;
import org.apache.solr.io.eval.TemporalEvaluatorSecond;
import org.apache.solr.io.eval.TemporalEvaluatorWeek;
import org.apache.solr.io.eval.TemporalEvaluatorYear;
import org.apache.solr.io.eval.TermVectorsEvaluator;
import org.apache.solr.io.eval.TimeDifferencingEvaluator;
import org.apache.solr.io.eval.TopFeaturesEvaluator;
import org.apache.solr.io.eval.TransposeEvaluator;
import org.apache.solr.io.eval.TriangularDistributionEvaluator;
import org.apache.solr.io.eval.TrimEvaluator;
import org.apache.solr.io.eval.TruncEvaluator;
import org.apache.solr.io.eval.UniformDistributionEvaluator;
import org.apache.solr.io.eval.UniformIntegerDistributionEvaluator;
import org.apache.solr.io.eval.UnitEvaluator;
import org.apache.solr.io.eval.UpperEvaluator;
import org.apache.solr.io.eval.UuidEvaluator;
import org.apache.solr.io.eval.ValueAtEvaluator;
import org.apache.solr.io.eval.VarianceEvaluator;
import org.apache.solr.io.eval.WeibullDistributionEvaluator;
import org.apache.solr.io.eval.ZerosEvaluator;
import org.apache.solr.io.eval.ZipFDistributionEvaluator;
import org.apache.solr.io.graph.GatherNodesStream;
import org.apache.solr.io.graph.ShortestPathStream;
import org.apache.solr.io.ops.DistinctOperation;
import org.apache.solr.io.ops.GroupOperation;
import org.apache.solr.io.ops.ReplaceOperation;
import org.apache.solr.io.ops.ReplaceWithFieldOperation;
import org.apache.solr.io.ops.ReplaceWithValueOperation;
import org.apache.solr.io.stream.CalculatorStream;
import org.apache.solr.io.stream.CartesianProductStream;
import org.apache.solr.io.stream.CellStream;
import org.apache.solr.io.stream.CommitStream;
import org.apache.solr.io.stream.ComplementStream;
import org.apache.solr.io.stream.CsvStream;
import org.apache.solr.io.stream.DaemonStream;
import org.apache.solr.io.stream.DeleteStream;
import org.apache.solr.io.stream.DrillStream;
import org.apache.solr.io.stream.EchoStream;
import org.apache.solr.io.stream.EvalStream;
import org.apache.solr.io.stream.ExecutorStream;
import org.apache.solr.io.stream.Facet2DStream;
import org.apache.solr.io.stream.FacetStream;
import org.apache.solr.io.stream.FeaturesSelectionStream;
import org.apache.solr.io.stream.FetchStream;
import org.apache.solr.io.stream.GetStream;
import org.apache.solr.io.stream.HashJoinStream;
import org.apache.solr.io.stream.HashRollupStream;
import org.apache.solr.io.stream.HavingStream;
import org.apache.solr.io.stream.InnerJoinStream;
import org.apache.solr.io.stream.IntersectStream;
import org.apache.solr.io.stream.KnnStream;
import org.apache.solr.io.stream.LeftOuterJoinStream;
import org.apache.solr.io.stream.LetStream;
import org.apache.solr.io.stream.ListStream;
import org.apache.solr.io.stream.MergeStream;
import org.apache.solr.io.stream.ModelStream;
import org.apache.solr.io.stream.NoOpStream;
import org.apache.solr.io.stream.NullStream;
import org.apache.solr.io.stream.OuterHashJoinStream;
import org.apache.solr.io.stream.ParallelListStream;
import org.apache.solr.io.stream.ParallelStream;
import org.apache.solr.io.stream.PlotStream;
import org.apache.solr.io.stream.PriorityStream;
import org.apache.solr.io.stream.RandomFacadeStream;
import org.apache.solr.io.stream.RankStream;
import org.apache.solr.io.stream.ReducerStream;
import org.apache.solr.io.stream.RollupStream;
import org.apache.solr.io.stream.ScoreNodesStream;
import org.apache.solr.io.stream.SearchFacadeStream;
import org.apache.solr.io.stream.SelectStream;
import org.apache.solr.io.stream.ShuffleStream;
import org.apache.solr.io.stream.SignificantTermsStream;
import org.apache.solr.io.stream.SortStream;
import org.apache.solr.io.stream.SqlStream;
import org.apache.solr.io.stream.StatsStream;
import org.apache.solr.io.stream.StreamContext;
import org.apache.solr.io.stream.TextLogitStream;
import org.apache.solr.io.stream.TimeSeriesStream;
import org.apache.solr.io.stream.TopicStream;
import org.apache.solr.io.stream.TsvStream;
import org.apache.solr.io.stream.TupStream;
import org.apache.solr.io.stream.TupleStream;
import org.apache.solr.io.stream.UniqueStream;
import org.apache.solr.io.stream.UpdateStream;
import org.apache.solr.io.stream.ZplotStream;
import org.apache.solr.io.stream.expr.Explanation;
import org.apache.solr.io.stream.expr.Expressible;
import org.apache.solr.io.stream.expr.StreamExplanation;
import org.apache.solr.io.stream.expr.StreamExpression;
import org.apache.solr.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.io.stream.expr.StreamFactory;
import org.apache.solr.io.stream.metrics.CountDistinctMetric;
import org.apache.solr.io.stream.metrics.CountMetric;
import org.apache.solr.io.stream.metrics.MaxMetric;
import org.apache.solr.io.stream.metrics.MeanMetric;
import org.apache.solr.io.stream.metrics.MinMetric;
import org.apache.solr.io.stream.metrics.PercentileMetric;
import org.apache.solr.io.stream.metrics.StdMetric;
import org.apache.solr.io.stream.metrics.SumMetric;
import org.apache.solr.common.params.CommonParams;

public class Lang {

  public static void register(StreamFactory streamFactory) {
    streamFactory
        // source streams
        .withFunctionName("search", SearchFacadeStream.class)
        .withFunctionName("facet", FacetStream.class)
        .withFunctionName("facet2D", Facet2DStream.class)
        .withFunctionName("update", UpdateStream.class)
        .withFunctionName("delete", DeleteStream.class)
        .withFunctionName("topic", TopicStream.class)
        .withFunctionName("commit", CommitStream.class)
        .withFunctionName("random", RandomFacadeStream.class)
        .withFunctionName("knnSearch", KnnStream.class)

        // decorator streams
        .withFunctionName("merge", MergeStream.class)
        .withFunctionName("unique", UniqueStream.class)
        .withFunctionName("top", RankStream.class)
        .withFunctionName("group", GroupOperation.class)
        .withFunctionName("reduce", ReducerStream.class)
        .withFunctionName("parallel", ParallelStream.class)
        .withFunctionName("rollup", RollupStream.class)
        .withFunctionName("stats", StatsStream.class)
        .withFunctionName("innerJoin", InnerJoinStream.class)
        .withFunctionName("leftOuterJoin", LeftOuterJoinStream.class)
        .withFunctionName("hashJoin", HashJoinStream.class)
        .withFunctionName("outerHashJoin", OuterHashJoinStream.class)
        .withFunctionName("intersect", IntersectStream.class)
        .withFunctionName("complement", ComplementStream.class)
        .withFunctionName("sort", SortStream.class)
        .withFunctionName("train", TextLogitStream.class)
        .withFunctionName("features", FeaturesSelectionStream.class)
        .withFunctionName("daemon", DaemonStream.class)
        .withFunctionName("shortestPath", ShortestPathStream.class)
        .withFunctionName("gatherNodes", GatherNodesStream.class)
        .withFunctionName("nodes", GatherNodesStream.class)
        .withFunctionName("select", SelectStream.class)
        .withFunctionName("shortestPath", ShortestPathStream.class)
        .withFunctionName("gatherNodes", GatherNodesStream.class)
        .withFunctionName("nodes", GatherNodesStream.class)
        .withFunctionName("scoreNodes", ScoreNodesStream.class)
        .withFunctionName("model", ModelStream.class)
        .withFunctionName("fetch", FetchStream.class)
        .withFunctionName("executor", ExecutorStream.class)
        .withFunctionName("null", NullStream.class)
        .withFunctionName("priority", PriorityStream.class)
        .withFunctionName("significantTerms", SignificantTermsStream.class)
        .withFunctionName("cartesianProduct", CartesianProductStream.class)
        .withFunctionName("shuffle", ShuffleStream.class)
        .withFunctionName("export", ShuffleStream.class)
        .withFunctionName("calc", CalculatorStream.class)
        .withFunctionName("eval", EvalStream.class)
        .withFunctionName("echo", EchoStream.class)
        .withFunctionName("cell", CellStream.class)
        .withFunctionName("list", ListStream.class)
        .withFunctionName("let", LetStream.class)
        .withFunctionName("get", GetStream.class)
        .withFunctionName("timeseries", TimeSeriesStream.class)
        .withFunctionName("tuple", TupStream.class)
        .withFunctionName("sql", SqlStream.class)
        .withFunctionName("plist", ParallelListStream.class)
        .withFunctionName("zplot", ZplotStream.class)
        .withFunctionName("hashRollup", HashRollupStream.class)
        .withFunctionName("noop", NoOpStream.class)

        // metrics
        .withFunctionName("min", MinMetric.class)
        .withFunctionName("max", MaxMetric.class)
        .withFunctionName("avg", MeanMetric.class)
        .withFunctionName("sum", SumMetric.class)
        .withFunctionName("per", PercentileMetric.class)
        .withFunctionName("std", StdMetric.class)
        .withFunctionName("count", CountMetric.class)
        .withFunctionName("countDist", CountDistinctMetric.class)

        // tuple manipulation operations
        .withFunctionName("replace", ReplaceOperation.class)
        .withFunctionName("withValue", ReplaceWithValueOperation.class)
        .withFunctionName("withField", ReplaceWithFieldOperation.class)

        // stream reduction operations
        .withFunctionName("group", GroupOperation.class)
        .withFunctionName("distinct", DistinctOperation.class)
        .withFunctionName("having", HavingStream.class)

        // Stream Evaluators
        .withFunctionName("val", RawValueEvaluator.class)

        // New Evaluators
        .withFunctionName("anova", AnovaEvaluator.class)
        .withFunctionName("array", ArrayEvaluator.class)
        .withFunctionName("col", ColumnEvaluator.class)
        .withFunctionName("conv", ConvolutionEvaluator.class)
        .withFunctionName("copyOfRange", CopyOfRangeEvaluator.class)
        .withFunctionName("copyOf", CopyOfEvaluator.class)
        .withFunctionName("cov", CovarianceEvaluator.class)
        .withFunctionName("corr", CorrelationEvaluator.class)
        .withFunctionName("describe", DescribeEvaluator.class)
        .withFunctionName("distance", DistanceEvaluator.class)
        .withFunctionName("empiricalDistribution", EmpiricalDistributionEvaluator.class)
        .withFunctionName("finddelay", FindDelayEvaluator.class)
        .withFunctionName("hist", HistogramEvaluator.class)
        .withFunctionName("length", LengthEvaluator.class)
        .withFunctionName("movingAvg", MovingAverageEvaluator.class)
        .withFunctionName("standardize", NormalizeEvaluator.class)
        .withFunctionName("percentile", PercentileEvaluator.class)
        .withFunctionName("predict", PredictEvaluator.class)
        .withFunctionName("rank", RankEvaluator.class)
        .withFunctionName("regress", RegressionEvaluator.class)
        .withFunctionName("rev", ReverseEvaluator.class)
        .withFunctionName("scale", ScaleEvaluator.class)
        .withFunctionName("sequence", SequenceEvaluator.class)
        .withFunctionName("addAll", AppendEvaluator.class)
        .withFunctionName("append", AppendEvaluator.class)
        .withFunctionName("plot", PlotStream.class)
        .withFunctionName("normalDistribution", NormalDistributionEvaluator.class)
        .withFunctionName("uniformDistribution", UniformDistributionEvaluator.class)
        .withFunctionName("sample", SampleEvaluator.class)
        .withFunctionName("kolmogorovSmirnov", KolmogorovSmirnovEvaluator.class)
        .withFunctionName("ks", KolmogorovSmirnovEvaluator.class)
        .withFunctionName("asc", AscEvaluator.class)
        .withFunctionName("cumulativeProbability", CumulativeProbabilityEvaluator.class)
        .withFunctionName("ebeAdd", EBEAddEvaluator.class)
        .withFunctionName("ebeSubtract", EBESubtractEvaluator.class)
        .withFunctionName("ebeMultiply", EBEMultiplyEvaluator.class)
        .withFunctionName("ebeDivide", EBEDivideEvaluator.class)
        .withFunctionName("dotProduct", DotProductEvaluator.class)
        .withFunctionName("cosineSimilarity", CosineSimilarityEvaluator.class)
        .withFunctionName("freqTable", FrequencyTableEvaluator.class)
        .withFunctionName("uniformIntegerDistribution", UniformIntegerDistributionEvaluator.class)
        .withFunctionName("binomialDistribution", BinomialDistributionEvaluator.class)
        .withFunctionName("poissonDistribution", PoissonDistributionEvaluator.class)
        .withFunctionName("enumeratedDistribution", EnumeratedDistributionEvaluator.class)
        .withFunctionName("probability", ProbabilityEvaluator.class)
        .withFunctionName("sumDifference", SumDifferenceEvaluator.class)
        .withFunctionName("meanDifference", MeanDifferenceEvaluator.class)
        .withFunctionName("primes", PrimesEvaluator.class)
        .withFunctionName("factorial", FactorialEvaluator.class)
        .withFunctionName("movingMedian", MovingMedianEvaluator.class)
        .withFunctionName("binomialCoefficient", BinomialCoefficientEvaluator.class)
        .withFunctionName("expMovingAvg", ExponentialMovingAverageEvaluator.class)
        .withFunctionName("monteCarlo", MonteCarloEvaluator.class)
        .withFunctionName("constantDistribution", ConstantDistributionEvaluator.class)
        .withFunctionName("weibullDistribution", WeibullDistributionEvaluator.class)
        .withFunctionName("mean", MeanEvaluator.class)
        .withFunctionName("var", VarianceEvaluator.class)
        .withFunctionName("stddev", StandardDeviationEvaluator.class)
        .withFunctionName("mode", ModeEvaluator.class)
        .withFunctionName("logNormalDistribution", LogNormalDistributionEvaluator.class)
        .withFunctionName("zipFDistribution", ZipFDistributionEvaluator.class)
        .withFunctionName("gammaDistribution", GammaDistributionEvaluator.class)
        .withFunctionName("betaDistribution", BetaDistributionEvaluator.class)
        .withFunctionName("polyfit", PolyFitEvaluator.class)
        .withFunctionName("harmonicFit", HarmonicFitEvaluator.class)
        .withFunctionName("harmfit", HarmonicFitEvaluator.class)
        .withFunctionName("loess", LoessEvaluator.class)
        .withFunctionName("matrix", MatrixEvaluator.class)
        .withFunctionName("transpose", TransposeEvaluator.class)
        .withFunctionName("unitize", UnitEvaluator.class)
        .withFunctionName("triangularDistribution", TriangularDistributionEvaluator.class)
        .withFunctionName("precision", PrecisionEvaluator.class)
        .withFunctionName("minMaxScale", MinMaxScaleEvaluator.class)
        .withFunctionName("markovChain", MarkovChainEvaluator.class)
        .withFunctionName("grandSum", GrandSumEvaluator.class)
        .withFunctionName("scalarAdd", ScalarAddEvaluator.class)
        .withFunctionName("scalarSubtract", ScalarSubtractEvaluator.class)
        .withFunctionName("scalarMultiply", ScalarMultiplyEvaluator.class)
        .withFunctionName("scalarDivide", ScalarDivideEvaluator.class)
        .withFunctionName("sumRows", SumRowsEvaluator.class)
        .withFunctionName("sumColumns", SumColumnsEvaluator.class)
        .withFunctionName("diff", TimeDifferencingEvaluator.class)
        .withFunctionName("corrPValues", CorrelationSignificanceEvaluator.class)
        .withFunctionName("normalizeSum", NormalizeSumEvaluator.class)
        .withFunctionName("geometricDistribution", GeometricDistributionEvaluator.class)
        .withFunctionName("olsRegress", OLSRegressionEvaluator.class)
        .withFunctionName("derivative", DerivativeEvaluator.class)
        .withFunctionName("spline", SplineEvaluator.class)
        .withFunctionName("ttest", TTestEvaluator.class)
        .withFunctionName("pairedTtest", PairedTTestEvaluator.class)
        .withFunctionName(
            "multiVariateNormalDistribution", MultiVariateNormalDistributionEvaluator.class)
        .withFunctionName("integral", IntegrateEvaluator.class)
        .withFunctionName("density", DensityEvaluator.class)
        .withFunctionName("mannWhitney", MannWhitneyUEvaluator.class)
        .withFunctionName("sumSq", SumSqEvaluator.class)
        .withFunctionName("akima", AkimaEvaluator.class)
        .withFunctionName("lerp", LerpEvaluator.class)
        .withFunctionName("chiSquareDataSet", ChiSquareDataSetEvaluator.class)
        .withFunctionName("gtestDataSet", GTestDataSetEvaluator.class)
        .withFunctionName("termVectors", TermVectorsEvaluator.class)
        .withFunctionName("getColumnLabels", GetColumnLabelsEvaluator.class)
        .withFunctionName("getRowLabels", GetRowLabelsEvaluator.class)
        .withFunctionName("getAttribute", GetAttributeEvaluator.class)
        .withFunctionName("kmeans", KmeansEvaluator.class)
        .withFunctionName("getCentroids", GetCentroidsEvaluator.class)
        .withFunctionName("getCluster", GetClusterEvaluator.class)
        .withFunctionName("topFeatures", TopFeaturesEvaluator.class)
        .withFunctionName("featureSelect", FeatureSelectEvaluator.class)
        .withFunctionName("rowAt", RowAtEvaluator.class)
        .withFunctionName("colAt", ColumnAtEvaluator.class)
        .withFunctionName("setColumnLabels", SetColumnLabelsEvaluator.class)
        .withFunctionName("setRowLabels", SetRowLabelsEvaluator.class)
        .withFunctionName("knn", KnnEvaluator.class)
        .withFunctionName("getAttributes", GetAttributesEvaluator.class)
        .withFunctionName("indexOf", IndexOfEvaluator.class)
        .withFunctionName("columnCount", ColumnCountEvaluator.class)
        .withFunctionName("rowCount", RowCountEvaluator.class)
        .withFunctionName("fuzzyKmeans", FuzzyKmeansEvaluator.class)
        .withFunctionName("getMembershipMatrix", GetMembershipMatrixEvaluator.class)
        .withFunctionName("multiKmeans", MultiKmeansEvaluator.class)
        .withFunctionName("l2norm", NormEvaluator.class)
        .withFunctionName("l1norm", L1NormEvaluator.class)
        .withFunctionName("linfnorm", LInfNormEvaluator.class)
        .withFunctionName("matrixMult", MatrixMultiplyEvaluator.class)
        .withFunctionName("bicubicSpline", BicubicSplineEvaluator.class)
        .withFunctionName("valueAt", ValueAtEvaluator.class)
        .withFunctionName("memset", MemsetEvaluator.class)
        .withFunctionName("fft", FFTEvaluator.class)
        .withFunctionName("ifft", IFFTEvaluator.class)
        .withFunctionName("manhattan", ManhattanEvaluator.class)
        .withFunctionName("canberra", CanberraEvaluator.class)
        .withFunctionName("earthMovers", EarthMoversEvaluator.class)
        .withFunctionName("euclidean", EuclideanEvaluator.class)
        .withFunctionName("chebyshev", ChebyshevEvaluator.class)
        .withFunctionName("ones", OnesEvaluator.class)
        .withFunctionName("zeros", ZerosEvaluator.class)
        .withFunctionName("getValue", GetValueEvaluator.class)
        .withFunctionName("setValue", SetValueEvaluator.class)
        .withFunctionName("knnRegress", KnnRegressionEvaluator.class)
        .withFunctionName("gaussfit", GaussFitEvaluator.class)
        .withFunctionName("outliers", OutliersEvaluator.class)
        .withFunctionName("stream", GetStream.class)
        .withFunctionName("putCache", PutCacheEvaluator.class)
        .withFunctionName("getCache", GetCacheEvaluator.class)
        .withFunctionName("removeCache", RemoveCacheEvaluator.class)
        .withFunctionName("listCache", ListCacheEvaluator.class)
        .withFunctionName("zscores", NormalizeEvaluator.class)
        .withFunctionName("latlonVectors", LatLonVectorsEvaluator.class)
        .withFunctionName("convexHull", ConvexHullEvaluator.class)
        .withFunctionName("getVertices", GetVerticesEvaluator.class)
        .withFunctionName("getBaryCenter", GetBaryCenterEvaluator.class)
        .withFunctionName("getArea", GetAreaEvaluator.class)
        .withFunctionName("getBoundarySize", GetBoundarySizeEvaluator.class)
        .withFunctionName("oscillate", OscillateEvaluator.class)
        .withFunctionName("getAmplitude", GetAmplitudeEvaluator.class)
        .withFunctionName("getPhase", GetPhaseEvaluator.class)
        .withFunctionName("getAngularFrequency", GetAngularFrequencyEvaluator.class)
        .withFunctionName("enclosingDisk", EnclosingDiskEvaluator.class)
        .withFunctionName("getCenter", GetCenterEvaluator.class)
        .withFunctionName("getRadius", GetRadiusEvaluator.class)
        .withFunctionName("getSupportPoints", GetSupportPointsEvaluator.class)
        .withFunctionName("pairSort", PairSortEvaluator.class)
        .withFunctionName("recip", RecipEvaluator.class)
        .withFunctionName("pivot", PivotEvaluator.class)
        .withFunctionName("drill", DrillStream.class)
        .withFunctionName("input", LocalInputStream.class)
        .withFunctionName("ltrim", LeftShiftEvaluator.class)
        .withFunctionName("rtrim", RightShiftEvaluator.class)
        .withFunctionName("repeat", RepeatEvaluator.class)
        .withFunctionName("natural", NaturalEvaluator.class)
        .withFunctionName("movingMAD", MovingMADEvaluator.class)
        .withFunctionName("recNum", RecNumEvaluator.class)
        .withFunctionName("notNull", NotNullEvaluator.class)
        .withFunctionName("isNull", IsNullEvaluator.class)
        .withFunctionName("matches", MatchesEvaluator.class)
        .withFunctionName("projectToBorder", ProjectToBorderEvaluator.class)
        .withFunctionName("parseCSV", CsvStream.class)
        .withFunctionName("parseTSV", TsvStream.class)
        .withFunctionName("double", DoubleEvaluator.class)
        .withFunctionName("long", LongEvaluator.class)
        .withFunctionName("dateTime", DateEvaluator.class)
        .withFunctionName("concat", ConcatEvaluator.class)
        .withFunctionName("lower", LowerEvaluator.class)
        .withFunctionName("upper", UpperEvaluator.class)
        .withFunctionName("split", SplitEvaluator.class)
        .withFunctionName("trim", TrimEvaluator.class)
        .withFunctionName("cosine", CosineDistanceEvaluator.class)
        .withFunctionName("trunc", TruncEvaluator.class)
        .withFunctionName("dbscan", DbscanEvaluator.class)
        // Boolean Stream Evaluators

        .withFunctionName("and", AndEvaluator.class)
        .withFunctionName("eor", ExclusiveOrEvaluator.class)
        .withFunctionName("eq", EqualToEvaluator.class)
        .withFunctionName("gt", GreaterThanEvaluator.class)
        .withFunctionName("gteq", GreaterThanEqualToEvaluator.class)
        .withFunctionName("lt", LessThanEvaluator.class)
        .withFunctionName("lteq", LessThanEqualToEvaluator.class)
        .withFunctionName("not", NotEvaluator.class)
        .withFunctionName("or", OrEvaluator.class)

        // Date Time Evaluators
        .withFunctionName(TemporalEvaluatorYear.FUNCTION_NAME, TemporalEvaluatorYear.class)
        .withFunctionName(TemporalEvaluatorMonth.FUNCTION_NAME, TemporalEvaluatorMonth.class)
        .withFunctionName(TemporalEvaluatorDay.FUNCTION_NAME, TemporalEvaluatorDay.class)
        .withFunctionName(
            TemporalEvaluatorDayOfYear.FUNCTION_NAME, TemporalEvaluatorDayOfYear.class)
        .withFunctionName(TemporalEvaluatorHour.FUNCTION_NAME, TemporalEvaluatorHour.class)
        .withFunctionName(TemporalEvaluatorMinute.FUNCTION_NAME, TemporalEvaluatorMinute.class)
        .withFunctionName(TemporalEvaluatorSecond.FUNCTION_NAME, TemporalEvaluatorSecond.class)
        .withFunctionName(TemporalEvaluatorEpoch.FUNCTION_NAME, TemporalEvaluatorEpoch.class)
        .withFunctionName(TemporalEvaluatorWeek.FUNCTION_NAME, TemporalEvaluatorWeek.class)
        .withFunctionName(TemporalEvaluatorQuarter.FUNCTION_NAME, TemporalEvaluatorQuarter.class)
        .withFunctionName(
            TemporalEvaluatorDayOfQuarter.FUNCTION_NAME, TemporalEvaluatorDayOfQuarter.class)

        // Number Stream Evaluators
        .withFunctionName("abs", AbsoluteValueEvaluator.class)
        .withFunctionName("add", AddEvaluator.class)
        .withFunctionName("div", DivideEvaluator.class)
        .withFunctionName("mult", MultiplyEvaluator.class)
        .withFunctionName("sub", SubtractEvaluator.class)
        .withFunctionName("log", NaturalLogEvaluator.class)
        .withFunctionName("log10", Log10Evaluator.class)
        .withFunctionName("pow", PowerEvaluator.class)
        .withFunctionName("mod", ModuloEvaluator.class)
        .withFunctionName("ceil", CeilingEvaluator.class)
        .withFunctionName("floor", FloorEvaluator.class)
        .withFunctionName("sin", SineEvaluator.class)
        .withFunctionName("asin", ArcSineEvaluator.class)
        .withFunctionName("sinh", HyperbolicSineEvaluator.class)
        .withFunctionName("cos", CosineEvaluator.class)
        .withFunctionName("acos", ArcCosineEvaluator.class)
        .withFunctionName("cosh", HyperbolicCosineEvaluator.class)
        .withFunctionName("tan", TangentEvaluator.class)
        .withFunctionName("atan", ArcTangentEvaluator.class)
        .withFunctionName("tanh", HyperbolicTangentEvaluator.class)
        .withFunctionName("round", RoundEvaluator.class)
        .withFunctionName("sqrt", SquareRootEvaluator.class)
        .withFunctionName("cbrt", CubedRootEvaluator.class)
        .withFunctionName("coalesce", CoalesceEvaluator.class)
        .withFunctionName("uuid", UuidEvaluator.class)

        // Conditional Stream Evaluators
        .withFunctionName("if", IfThenElseEvaluator.class)
        .withFunctionName("convert", ConversionEvaluator.class);
  }

  public static class LocalInputStream extends TupleStream implements Expressible {

    private StreamComparator streamComparator;
    private String sort;

    public LocalInputStream(StreamExpression expression, StreamFactory factory) throws IOException {
      this.streamComparator = parseComp(factory.getDefaultSort());
    }

    @Override
    public void setStreamContext(StreamContext context) {
      sort = (String) context.get(CommonParams.SORT);
    }

    @Override
    public List<TupleStream> children() {
      return null;
    }

    private StreamComparator parseComp(String sort) throws IOException {

      String[] sorts = sort.split(",");
      StreamComparator[] comps = new StreamComparator[sorts.length];
      for (int i = 0; i < sorts.length; i++) {
        String s = sorts[i];

        String[] spec =
            s.trim().split("\\s+"); // This should take into account spaces in the sort spec.

        if (spec.length != 2) {
          throw new IOException("Invalid sort spec:" + s);
        }

        String fieldName = spec[0].trim();
        String order = spec[1].trim();

        comps[i] =
            new FieldComparator(
                fieldName,
                order.equalsIgnoreCase("asc")
                    ? ComparatorOrder.ASCENDING
                    : ComparatorOrder.DESCENDING);
      }

      if (comps.length > 1) {
        return new MultipleFieldComparator(comps);
      } else {
        return comps[0];
      }
    }

    @Override
    public void open() throws IOException {
      streamComparator = parseComp(sort);
    }

    @Override
    public void close() throws IOException {}

    @Override
    public Tuple read() throws IOException {
      return null;
    }

    @Override
    public StreamComparator getStreamSort() {
      return streamComparator;
    }

    @Override
    public StreamExpressionParameter toExpression(StreamFactory factory) throws IOException {
      StreamExpression expression = new StreamExpression(factory.getFunctionName(this.getClass()));
      return expression;
    }

    @Override
    public Explanation toExplanation(StreamFactory factory) throws IOException {
      return new StreamExplanation(getStreamNodeId().toString())
          .withFunctionName("input")
          .withImplementingClass(this.getClass().getName())
          .withExpressionType(Explanation.ExpressionType.STREAM_SOURCE)
          .withExpression("--non-expressible--");
    }
  }
}
