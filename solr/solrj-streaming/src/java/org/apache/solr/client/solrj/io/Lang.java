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
package org.apache.solr.client.solrj.io;

import java.io.IOException;
import java.util.List;
import org.apache.solr.client.solrj.io.comp.ComparatorOrder;
import org.apache.solr.client.solrj.io.comp.FieldComparator;
import org.apache.solr.client.solrj.io.comp.MultipleFieldComparator;
import org.apache.solr.client.solrj.io.comp.StreamComparator;
import org.apache.solr.client.solrj.io.eval.AbsoluteValueEvaluator;
import org.apache.solr.client.solrj.io.eval.AddEvaluator;
import org.apache.solr.client.solrj.io.eval.AkimaEvaluator;
import org.apache.solr.client.solrj.io.eval.AndEvaluator;
import org.apache.solr.client.solrj.io.eval.AnovaEvaluator;
import org.apache.solr.client.solrj.io.eval.AppendEvaluator;
import org.apache.solr.client.solrj.io.eval.ArcCosineEvaluator;
import org.apache.solr.client.solrj.io.eval.ArcSineEvaluator;
import org.apache.solr.client.solrj.io.eval.ArcTangentEvaluator;
import org.apache.solr.client.solrj.io.eval.ArrayEvaluator;
import org.apache.solr.client.solrj.io.eval.AscEvaluator;
import org.apache.solr.client.solrj.io.eval.BetaDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.BicubicSplineEvaluator;
import org.apache.solr.client.solrj.io.eval.BinomialCoefficientEvaluator;
import org.apache.solr.client.solrj.io.eval.BinomialDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.CanberraEvaluator;
import org.apache.solr.client.solrj.io.eval.CeilingEvaluator;
import org.apache.solr.client.solrj.io.eval.ChebyshevEvaluator;
import org.apache.solr.client.solrj.io.eval.ChiSquareDataSetEvaluator;
import org.apache.solr.client.solrj.io.eval.CoalesceEvaluator;
import org.apache.solr.client.solrj.io.eval.ColumnAtEvaluator;
import org.apache.solr.client.solrj.io.eval.ColumnCountEvaluator;
import org.apache.solr.client.solrj.io.eval.ColumnEvaluator;
import org.apache.solr.client.solrj.io.eval.ConcatEvaluator;
import org.apache.solr.client.solrj.io.eval.ConstantDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.ConversionEvaluator;
import org.apache.solr.client.solrj.io.eval.ConvexHullEvaluator;
import org.apache.solr.client.solrj.io.eval.ConvolutionEvaluator;
import org.apache.solr.client.solrj.io.eval.CopyOfEvaluator;
import org.apache.solr.client.solrj.io.eval.CopyOfRangeEvaluator;
import org.apache.solr.client.solrj.io.eval.CorrelationEvaluator;
import org.apache.solr.client.solrj.io.eval.CorrelationSignificanceEvaluator;
import org.apache.solr.client.solrj.io.eval.CosineDistanceEvaluator;
import org.apache.solr.client.solrj.io.eval.CosineEvaluator;
import org.apache.solr.client.solrj.io.eval.CosineSimilarityEvaluator;
import org.apache.solr.client.solrj.io.eval.CovarianceEvaluator;
import org.apache.solr.client.solrj.io.eval.CubedRootEvaluator;
import org.apache.solr.client.solrj.io.eval.CumulativeProbabilityEvaluator;
import org.apache.solr.client.solrj.io.eval.DateEvaluator;
import org.apache.solr.client.solrj.io.eval.DbscanEvaluator;
import org.apache.solr.client.solrj.io.eval.DensityEvaluator;
import org.apache.solr.client.solrj.io.eval.DerivativeEvaluator;
import org.apache.solr.client.solrj.io.eval.DescribeEvaluator;
import org.apache.solr.client.solrj.io.eval.DistanceEvaluator;
import org.apache.solr.client.solrj.io.eval.DivideEvaluator;
import org.apache.solr.client.solrj.io.eval.DotProductEvaluator;
import org.apache.solr.client.solrj.io.eval.DoubleEvaluator;
import org.apache.solr.client.solrj.io.eval.EBEAddEvaluator;
import org.apache.solr.client.solrj.io.eval.EBEDivideEvaluator;
import org.apache.solr.client.solrj.io.eval.EBEMultiplyEvaluator;
import org.apache.solr.client.solrj.io.eval.EBESubtractEvaluator;
import org.apache.solr.client.solrj.io.eval.EarthMoversEvaluator;
import org.apache.solr.client.solrj.io.eval.EmpiricalDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.EnclosingDiskEvaluator;
import org.apache.solr.client.solrj.io.eval.EnumeratedDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.EqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.EuclideanEvaluator;
import org.apache.solr.client.solrj.io.eval.ExclusiveOrEvaluator;
import org.apache.solr.client.solrj.io.eval.ExponentialMovingAverageEvaluator;
import org.apache.solr.client.solrj.io.eval.FFTEvaluator;
import org.apache.solr.client.solrj.io.eval.FactorialEvaluator;
import org.apache.solr.client.solrj.io.eval.FeatureSelectEvaluator;
import org.apache.solr.client.solrj.io.eval.FindDelayEvaluator;
import org.apache.solr.client.solrj.io.eval.FloorEvaluator;
import org.apache.solr.client.solrj.io.eval.FrequencyTableEvaluator;
import org.apache.solr.client.solrj.io.eval.FuzzyKmeansEvaluator;
import org.apache.solr.client.solrj.io.eval.GTestDataSetEvaluator;
import org.apache.solr.client.solrj.io.eval.GammaDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.GaussFitEvaluator;
import org.apache.solr.client.solrj.io.eval.GeometricDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.GetAmplitudeEvaluator;
import org.apache.solr.client.solrj.io.eval.GetAngularFrequencyEvaluator;
import org.apache.solr.client.solrj.io.eval.GetAreaEvaluator;
import org.apache.solr.client.solrj.io.eval.GetAttributeEvaluator;
import org.apache.solr.client.solrj.io.eval.GetAttributesEvaluator;
import org.apache.solr.client.solrj.io.eval.GetBaryCenterEvaluator;
import org.apache.solr.client.solrj.io.eval.GetBoundarySizeEvaluator;
import org.apache.solr.client.solrj.io.eval.GetCacheEvaluator;
import org.apache.solr.client.solrj.io.eval.GetCenterEvaluator;
import org.apache.solr.client.solrj.io.eval.GetCentroidsEvaluator;
import org.apache.solr.client.solrj.io.eval.GetClusterEvaluator;
import org.apache.solr.client.solrj.io.eval.GetColumnLabelsEvaluator;
import org.apache.solr.client.solrj.io.eval.GetMembershipMatrixEvaluator;
import org.apache.solr.client.solrj.io.eval.GetPhaseEvaluator;
import org.apache.solr.client.solrj.io.eval.GetRadiusEvaluator;
import org.apache.solr.client.solrj.io.eval.GetRowLabelsEvaluator;
import org.apache.solr.client.solrj.io.eval.GetSupportPointsEvaluator;
import org.apache.solr.client.solrj.io.eval.GetValueEvaluator;
import org.apache.solr.client.solrj.io.eval.GetVerticesEvaluator;
import org.apache.solr.client.solrj.io.eval.GrandSumEvaluator;
import org.apache.solr.client.solrj.io.eval.GreaterThanEqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.GreaterThanEvaluator;
import org.apache.solr.client.solrj.io.eval.HarmonicFitEvaluator;
import org.apache.solr.client.solrj.io.eval.HistogramEvaluator;
import org.apache.solr.client.solrj.io.eval.HyperbolicCosineEvaluator;
import org.apache.solr.client.solrj.io.eval.HyperbolicSineEvaluator;
import org.apache.solr.client.solrj.io.eval.HyperbolicTangentEvaluator;
import org.apache.solr.client.solrj.io.eval.IFFTEvaluator;
import org.apache.solr.client.solrj.io.eval.IfThenElseEvaluator;
import org.apache.solr.client.solrj.io.eval.IndexOfEvaluator;
import org.apache.solr.client.solrj.io.eval.IntegrateEvaluator;
import org.apache.solr.client.solrj.io.eval.IsNullEvaluator;
import org.apache.solr.client.solrj.io.eval.KmeansEvaluator;
import org.apache.solr.client.solrj.io.eval.KnnEvaluator;
import org.apache.solr.client.solrj.io.eval.KnnRegressionEvaluator;
import org.apache.solr.client.solrj.io.eval.KolmogorovSmirnovEvaluator;
import org.apache.solr.client.solrj.io.eval.L1NormEvaluator;
import org.apache.solr.client.solrj.io.eval.LInfNormEvaluator;
import org.apache.solr.client.solrj.io.eval.LatLonVectorsEvaluator;
import org.apache.solr.client.solrj.io.eval.LeftShiftEvaluator;
import org.apache.solr.client.solrj.io.eval.LengthEvaluator;
import org.apache.solr.client.solrj.io.eval.LerpEvaluator;
import org.apache.solr.client.solrj.io.eval.LessThanEqualToEvaluator;
import org.apache.solr.client.solrj.io.eval.LessThanEvaluator;
import org.apache.solr.client.solrj.io.eval.ListCacheEvaluator;
import org.apache.solr.client.solrj.io.eval.LoessEvaluator;
import org.apache.solr.client.solrj.io.eval.Log10Evaluator;
import org.apache.solr.client.solrj.io.eval.LogNormalDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.LongEvaluator;
import org.apache.solr.client.solrj.io.eval.LowerEvaluator;
import org.apache.solr.client.solrj.io.eval.ManhattanEvaluator;
import org.apache.solr.client.solrj.io.eval.MannWhitneyUEvaluator;
import org.apache.solr.client.solrj.io.eval.MarkovChainEvaluator;
import org.apache.solr.client.solrj.io.eval.MatchesEvaluator;
import org.apache.solr.client.solrj.io.eval.MatrixEvaluator;
import org.apache.solr.client.solrj.io.eval.MatrixMultiplyEvaluator;
import org.apache.solr.client.solrj.io.eval.MeanDifferenceEvaluator;
import org.apache.solr.client.solrj.io.eval.MeanEvaluator;
import org.apache.solr.client.solrj.io.eval.MemsetEvaluator;
import org.apache.solr.client.solrj.io.eval.MinMaxScaleEvaluator;
import org.apache.solr.client.solrj.io.eval.ModeEvaluator;
import org.apache.solr.client.solrj.io.eval.ModuloEvaluator;
import org.apache.solr.client.solrj.io.eval.MonteCarloEvaluator;
import org.apache.solr.client.solrj.io.eval.MovingAverageEvaluator;
import org.apache.solr.client.solrj.io.eval.MovingMADEvaluator;
import org.apache.solr.client.solrj.io.eval.MovingMedianEvaluator;
import org.apache.solr.client.solrj.io.eval.MultiKmeansEvaluator;
import org.apache.solr.client.solrj.io.eval.MultiVariateNormalDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.MultiplyEvaluator;
import org.apache.solr.client.solrj.io.eval.NaturalEvaluator;
import org.apache.solr.client.solrj.io.eval.NaturalLogEvaluator;
import org.apache.solr.client.solrj.io.eval.NormEvaluator;
import org.apache.solr.client.solrj.io.eval.NormalDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.NormalizeEvaluator;
import org.apache.solr.client.solrj.io.eval.NormalizeSumEvaluator;
import org.apache.solr.client.solrj.io.eval.NotEvaluator;
import org.apache.solr.client.solrj.io.eval.NotNullEvaluator;
import org.apache.solr.client.solrj.io.eval.OLSRegressionEvaluator;
import org.apache.solr.client.solrj.io.eval.OnesEvaluator;
import org.apache.solr.client.solrj.io.eval.OrEvaluator;
import org.apache.solr.client.solrj.io.eval.OscillateEvaluator;
import org.apache.solr.client.solrj.io.eval.OutliersEvaluator;
import org.apache.solr.client.solrj.io.eval.PairSortEvaluator;
import org.apache.solr.client.solrj.io.eval.PairedTTestEvaluator;
import org.apache.solr.client.solrj.io.eval.PercentileEvaluator;
import org.apache.solr.client.solrj.io.eval.PivotEvaluator;
import org.apache.solr.client.solrj.io.eval.PoissonDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.PolyFitEvaluator;
import org.apache.solr.client.solrj.io.eval.PowerEvaluator;
import org.apache.solr.client.solrj.io.eval.PrecisionEvaluator;
import org.apache.solr.client.solrj.io.eval.PredictEvaluator;
import org.apache.solr.client.solrj.io.eval.PrimesEvaluator;
import org.apache.solr.client.solrj.io.eval.ProbabilityEvaluator;
import org.apache.solr.client.solrj.io.eval.ProjectToBorderEvaluator;
import org.apache.solr.client.solrj.io.eval.PutCacheEvaluator;
import org.apache.solr.client.solrj.io.eval.RankEvaluator;
import org.apache.solr.client.solrj.io.eval.RawValueEvaluator;
import org.apache.solr.client.solrj.io.eval.RecNumEvaluator;
import org.apache.solr.client.solrj.io.eval.RecipEvaluator;
import org.apache.solr.client.solrj.io.eval.RegressionEvaluator;
import org.apache.solr.client.solrj.io.eval.RemoveCacheEvaluator;
import org.apache.solr.client.solrj.io.eval.RepeatEvaluator;
import org.apache.solr.client.solrj.io.eval.ReverseEvaluator;
import org.apache.solr.client.solrj.io.eval.RightShiftEvaluator;
import org.apache.solr.client.solrj.io.eval.RoundEvaluator;
import org.apache.solr.client.solrj.io.eval.RowAtEvaluator;
import org.apache.solr.client.solrj.io.eval.RowCountEvaluator;
import org.apache.solr.client.solrj.io.eval.SampleEvaluator;
import org.apache.solr.client.solrj.io.eval.ScalarAddEvaluator;
import org.apache.solr.client.solrj.io.eval.ScalarDivideEvaluator;
import org.apache.solr.client.solrj.io.eval.ScalarMultiplyEvaluator;
import org.apache.solr.client.solrj.io.eval.ScalarSubtractEvaluator;
import org.apache.solr.client.solrj.io.eval.ScaleEvaluator;
import org.apache.solr.client.solrj.io.eval.SequenceEvaluator;
import org.apache.solr.client.solrj.io.eval.SetColumnLabelsEvaluator;
import org.apache.solr.client.solrj.io.eval.SetRowLabelsEvaluator;
import org.apache.solr.client.solrj.io.eval.SetValueEvaluator;
import org.apache.solr.client.solrj.io.eval.SineEvaluator;
import org.apache.solr.client.solrj.io.eval.SplineEvaluator;
import org.apache.solr.client.solrj.io.eval.SplitEvaluator;
import org.apache.solr.client.solrj.io.eval.SquareRootEvaluator;
import org.apache.solr.client.solrj.io.eval.StandardDeviationEvaluator;
import org.apache.solr.client.solrj.io.eval.SubtractEvaluator;
import org.apache.solr.client.solrj.io.eval.SumColumnsEvaluator;
import org.apache.solr.client.solrj.io.eval.SumDifferenceEvaluator;
import org.apache.solr.client.solrj.io.eval.SumRowsEvaluator;
import org.apache.solr.client.solrj.io.eval.SumSqEvaluator;
import org.apache.solr.client.solrj.io.eval.TTestEvaluator;
import org.apache.solr.client.solrj.io.eval.TangentEvaluator;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDay;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDayOfQuarter;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorDayOfYear;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorEpoch;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorHour;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorMinute;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorMonth;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorQuarter;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorSecond;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorWeek;
import org.apache.solr.client.solrj.io.eval.TemporalEvaluatorYear;
import org.apache.solr.client.solrj.io.eval.TermVectorsEvaluator;
import org.apache.solr.client.solrj.io.eval.TimeDifferencingEvaluator;
import org.apache.solr.client.solrj.io.eval.TopFeaturesEvaluator;
import org.apache.solr.client.solrj.io.eval.TransposeEvaluator;
import org.apache.solr.client.solrj.io.eval.TriangularDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.TrimEvaluator;
import org.apache.solr.client.solrj.io.eval.TruncEvaluator;
import org.apache.solr.client.solrj.io.eval.UniformDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.UniformIntegerDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.UnitEvaluator;
import org.apache.solr.client.solrj.io.eval.UpperEvaluator;
import org.apache.solr.client.solrj.io.eval.UuidEvaluator;
import org.apache.solr.client.solrj.io.eval.ValueAtEvaluator;
import org.apache.solr.client.solrj.io.eval.VarianceEvaluator;
import org.apache.solr.client.solrj.io.eval.WeibullDistributionEvaluator;
import org.apache.solr.client.solrj.io.eval.ZerosEvaluator;
import org.apache.solr.client.solrj.io.eval.ZipFDistributionEvaluator;
import org.apache.solr.client.solrj.io.graph.GatherNodesStream;
import org.apache.solr.client.solrj.io.graph.ShortestPathStream;
import org.apache.solr.client.solrj.io.ops.DistinctOperation;
import org.apache.solr.client.solrj.io.ops.GroupOperation;
import org.apache.solr.client.solrj.io.ops.ReplaceOperation;
import org.apache.solr.client.solrj.io.ops.ReplaceWithFieldOperation;
import org.apache.solr.client.solrj.io.ops.ReplaceWithValueOperation;
import org.apache.solr.client.solrj.io.stream.CalculatorStream;
import org.apache.solr.client.solrj.io.stream.CartesianProductStream;
import org.apache.solr.client.solrj.io.stream.CellStream;
import org.apache.solr.client.solrj.io.stream.CommitStream;
import org.apache.solr.client.solrj.io.stream.ComplementStream;
import org.apache.solr.client.solrj.io.stream.CsvStream;
import org.apache.solr.client.solrj.io.stream.DaemonStream;
import org.apache.solr.client.solrj.io.stream.DeleteStream;
import org.apache.solr.client.solrj.io.stream.DrillStream;
import org.apache.solr.client.solrj.io.stream.EchoStream;
import org.apache.solr.client.solrj.io.stream.EvalStream;
import org.apache.solr.client.solrj.io.stream.ExecutorStream;
import org.apache.solr.client.solrj.io.stream.Facet2DStream;
import org.apache.solr.client.solrj.io.stream.FacetStream;
import org.apache.solr.client.solrj.io.stream.FeaturesSelectionStream;
import org.apache.solr.client.solrj.io.stream.FetchStream;
import org.apache.solr.client.solrj.io.stream.GetStream;
import org.apache.solr.client.solrj.io.stream.HashJoinStream;
import org.apache.solr.client.solrj.io.stream.HashRollupStream;
import org.apache.solr.client.solrj.io.stream.HavingStream;
import org.apache.solr.client.solrj.io.stream.InnerJoinStream;
import org.apache.solr.client.solrj.io.stream.IntersectStream;
import org.apache.solr.client.solrj.io.stream.KnnStream;
import org.apache.solr.client.solrj.io.stream.LeftOuterJoinStream;
import org.apache.solr.client.solrj.io.stream.LetStream;
import org.apache.solr.client.solrj.io.stream.ListStream;
import org.apache.solr.client.solrj.io.stream.MergeStream;
import org.apache.solr.client.solrj.io.stream.ModelStream;
import org.apache.solr.client.solrj.io.stream.NoOpStream;
import org.apache.solr.client.solrj.io.stream.NullStream;
import org.apache.solr.client.solrj.io.stream.OuterHashJoinStream;
import org.apache.solr.client.solrj.io.stream.ParallelListStream;
import org.apache.solr.client.solrj.io.stream.ParallelStream;
import org.apache.solr.client.solrj.io.stream.PlotStream;
import org.apache.solr.client.solrj.io.stream.PriorityStream;
import org.apache.solr.client.solrj.io.stream.RandomFacadeStream;
import org.apache.solr.client.solrj.io.stream.RankStream;
import org.apache.solr.client.solrj.io.stream.ReducerStream;
import org.apache.solr.client.solrj.io.stream.RollupStream;
import org.apache.solr.client.solrj.io.stream.ScoreNodesStream;
import org.apache.solr.client.solrj.io.stream.SearchFacadeStream;
import org.apache.solr.client.solrj.io.stream.SelectStream;
import org.apache.solr.client.solrj.io.stream.ShuffleStream;
import org.apache.solr.client.solrj.io.stream.SignificantTermsStream;
import org.apache.solr.client.solrj.io.stream.SortStream;
import org.apache.solr.client.solrj.io.stream.SqlStream;
import org.apache.solr.client.solrj.io.stream.StatsStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TextLogitStream;
import org.apache.solr.client.solrj.io.stream.TimeSeriesStream;
import org.apache.solr.client.solrj.io.stream.TopicStream;
import org.apache.solr.client.solrj.io.stream.TsvStream;
import org.apache.solr.client.solrj.io.stream.TupStream;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.UniqueStream;
import org.apache.solr.client.solrj.io.stream.UpdateStream;
import org.apache.solr.client.solrj.io.stream.ZplotStream;
import org.apache.solr.client.solrj.io.stream.expr.Explanation;
import org.apache.solr.client.solrj.io.stream.expr.Expressible;
import org.apache.solr.client.solrj.io.stream.expr.StreamExplanation;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.io.stream.metrics.CountDistinctMetric;
import org.apache.solr.client.solrj.io.stream.metrics.CountMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MaxMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MeanMetric;
import org.apache.solr.client.solrj.io.stream.metrics.MinMetric;
import org.apache.solr.client.solrj.io.stream.metrics.PercentileMetric;
import org.apache.solr.client.solrj.io.stream.metrics.StdMetric;
import org.apache.solr.client.solrj.io.stream.metrics.SumMetric;
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
