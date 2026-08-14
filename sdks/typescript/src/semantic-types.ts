// Field-level DTOs generated from the checked-in semantic service models.
// Do not edit by hand; run scripts/generate-semantic-dtos.mjs.

export interface AccessDeniedException {
  message: string;
}

export type Boolean = boolean;

export interface BuildGraphIndexInput {
  graphName: string;
}

export interface BuildGraphIndexOutput {
  index: boolean;
}

export type Confidence = number;

export interface ConflictException {
  message: string;
}

export interface CreateGraphInput {
  graphName: string;
}

export interface CreateGraphOutput {
}

export interface CreateIndexInput {
  vectorBucketName?: string;
  vectorBucketArn?: string;
  indexName: string;
  dataType: DataType;
  dimension: number;
  distanceMetric: DistanceMetric;
  metadataConfiguration?: MetadataConfiguration;
  encryptionConfiguration?: EncryptionConfiguration;
  tags?: Record<string, string>;
}

export interface CreateIndexOutput {
  indexArn: string;
}

export interface CreateVectorBucketInput {
  vectorBucketName: string;
  encryptionConfiguration?: EncryptionConfiguration;
  tags?: Record<string, string>;
}

export interface CreateVectorBucketOutput {
  vectorBucketArn: string;
}

export type DataType = "float32";

export interface DeleteGraphInput {
  graphName: string;
}

export interface DeleteGraphOutput {
}

export interface DeleteIndexInput {
  vectorBucketName?: string;
  indexName?: string;
  indexArn?: string;
}

export interface DeleteIndexOutput {
}

export interface DeleteVectorBucketInput {
  vectorBucketName?: string;
  vectorBucketArn?: string;
}

export interface DeleteVectorBucketOutput {
}

export interface DeleteVectorBucketPolicyInput {
  vectorBucketName?: string;
  vectorBucketArn?: string;
}

export interface DeleteVectorBucketPolicyOutput {
}

export interface DeleteVectorsInput {
  vectorBucketName?: string;
  indexName?: string;
  indexArn?: string;
  keys: string[];
}

export type DeleteVectorsInputList = string[];

export interface DeleteVectorsOutput {
}

export type Dimension = number;

export type Direction = "out" | "in" | "both";

export type DistanceMetric = "euclidean" | "cosine";

export interface Edge {
  confidence?: number;
  edgeId?: string;
  predicate: string;
  properties?: Record<string, unknown>;
  provenance: string;
  sourceId: string;
  targetId: string;
}

export type EdgeId = string;

export type EdgeList = Edge[];

export interface EncryptionConfiguration {
  sseType?: SseType;
  kmsKeyArn?: string;
}

export type ErrorMessage = string;

export type ExceptionMessage = string;

export type Float = number;

export type Float32VectorData = number[];

export interface GetGraphInput {
  graphName: string;
}

export interface GetGraphOutput {
  edgesSnapshotId?: number;
  graphName: string;
}

export interface GetIndexInput {
  vectorBucketName?: string;
  indexName?: string;
  indexArn?: string;
}

export interface GetIndexOutput {
  index: Index;
}

export interface GetNeighborsInput {
  direction?: Direction;
  filter?: TraversalFilter;
  graphName: string;
  nodeId: string;
}

export interface GetNeighborsOutput {
  neighbors: Neighbor[];
}

export interface GetOutputVector {
  key: string;
  data?: VectorData;
  metadata?: unknown;
}

export interface GetVectorBucketInput {
  vectorBucketName?: string;
  vectorBucketArn?: string;
}

export interface GetVectorBucketOutput {
  vectorBucket: VectorBucket;
}

export interface GetVectorBucketPolicyInput {
  vectorBucketName?: string;
  vectorBucketArn?: string;
}

export interface GetVectorBucketPolicyOutput {
  policy?: string;
}

export interface GetVectorsInput {
  vectorBucketName?: string;
  indexName?: string;
  indexArn?: string;
  keys: string[];
  returnData?: boolean;
  returnMetadata?: boolean;
}

export type GetVectorsInputList = string[];

export interface GetVectorsOutput {
  vectors: GetOutputVector[];
}

export type GetVectorsOutputList = GetOutputVector[];

export type GraphName = string;

export interface GraphSummary {
  graphName: string;
}

export type GraphSummaryList = GraphSummary[];

export type HopCount = number;

export interface Index {
  vectorBucketName: string;
  indexName: string;
  indexArn: string;
  creationTime: string;
  dataType: DataType;
  dimension: number;
  distanceMetric: DistanceMetric;
  metadataConfiguration?: MetadataConfiguration;
  encryptionConfiguration?: EncryptionConfiguration;
}

export type IndexArn = string;

export type IndexBuilt = boolean;

export type IndexName = string;

export interface IndexSummary {
  vectorBucketName: string;
  indexName: string;
  indexArn: string;
  creationTime: string;
}

export interface InternalServerException {
  message: string;
}

export interface KmsDisabledException {
  message: string;
}

export interface KmsInvalidKeyUsageException {
  message: string;
}

export interface KmsInvalidStateException {
  message: string;
}

export type KmsKeyArn = string;

export interface KmsNotFoundException {
  message: string;
}

export type Label = string;

export type LabelList = string[];

export interface ListGraphsInput {
  maxResults?: number;
  nextToken?: string;
}

export interface ListGraphsOutput {
  graphs: GraphSummary[];
  nextToken?: string;
}

export interface ListIndexesInput {
  vectorBucketName?: string;
  vectorBucketArn?: string;
  maxResults?: number;
  nextToken?: string;
  prefix?: string;
}

export type ListIndexesMaxResults = number;

export type ListIndexesNextToken = string;

export interface ListIndexesOutput {
  nextToken?: string;
  indexes: IndexSummary[];
}

export type ListIndexesOutputList = IndexSummary[];

export type ListIndexesPrefix = string;

export interface ListOutputVector {
  key: string;
  data?: VectorData;
  metadata?: unknown;
}

export interface ListTagsForResourceInput {
  resourceArn: string;
}

export interface ListTagsForResourceOutput {
  tags: Record<string, string>;
}

export interface ListVectorBucketsInput {
  maxResults?: number;
  nextToken?: string;
  prefix?: string;
}

export type ListVectorBucketsMaxResults = number;

export type ListVectorBucketsNextToken = string;

export interface ListVectorBucketsOutput {
  nextToken?: string;
  vectorBuckets: VectorBucketSummary[];
}

export type ListVectorBucketsOutputList = VectorBucketSummary[];

export type ListVectorBucketsPrefix = string;

export interface ListVectorsInput {
  vectorBucketName?: string;
  indexName?: string;
  indexArn?: string;
  maxResults?: number;
  nextToken?: string;
  segmentCount?: number;
  segmentIndex?: number;
  returnData?: boolean;
  returnMetadata?: boolean;
}

export type ListVectorsMaxResults = number;

export type ListVectorsNextToken = string;

export interface ListVectorsOutput {
  nextToken?: string;
  vectors: ListOutputVector[];
}

export type ListVectorsOutputList = ListOutputVector[];

export type ListVectorsSegmentCount = number;

export type ListVectorsSegmentIndex = number;

export type MaxResults = number;

export interface MetadataConfiguration {
  nonFilterableMetadataKeys: string[];
}

export type MetadataKey = string;

export interface Neighbor {
  confidence: number;
  direction: Direction;
  edgeId: string;
  nodeId: string;
  predicate: string;
  provenance: string;
}

export type NeighborList = Neighbor[];

export interface Node {
  id: string;
  labels?: string[];
  properties?: Record<string, unknown>;
}

export type NodeId = string;

export type NodeIdList = string[];

export type NodeList = Node[];

export type NonFilterableMetadataKeys = string[];

export interface NotFoundException {
  message: string;
}

export type PaginationToken = string;

export interface Path {
  confidence: number;
  edges: TripletReceipt[];
  nodes: string[];
}

export type PathList = Path[];

export type Predicate = string;

export type Properties = Record<string, unknown>;

export type PropertyName = string;

export type Provenance = string;

export interface PutEdgesInput {
  edges: Edge[];
  graphName: string;
}

export interface PutEdgesOutput {
  snapshotId?: number;
}

export interface PutInputVector {
  key: string;
  data: VectorData;
  metadata?: unknown;
}

export interface PutNodesInput {
  graphName: string;
  nodes: Node[];
}

export interface PutNodesOutput {
  snapshotId?: number;
}

export interface PutVectorBucketPolicyInput {
  vectorBucketName?: string;
  vectorBucketArn?: string;
  policy: string;
}

export interface PutVectorBucketPolicyOutput {
}

export interface PutVectorsInput {
  vectorBucketName?: string;
  indexName?: string;
  indexArn?: string;
  vectors: PutInputVector[];
}

export type PutVectorsInputList = PutInputVector[];

export interface PutVectorsOutput {
}

export interface QueryKHopInput {
  direction?: Direction;
  filter?: TraversalFilter;
  graphName: string;
  k: number;
  nodeId: string;
}

export interface QueryKHopOutput {
  nodes: ReachedNode[];
}

export interface QueryNeighborhoodInput {
  direction?: Direction;
  filter?: TraversalFilter;
  graphName: string;
  k: number;
  nodeId: string;
}

export interface QueryNeighborhoodOutput {
  neighborhood: Subgraph;
}

export interface QueryOutputVector {
  distance?: number;
  key: string;
  metadata?: unknown;
}

export interface QueryPathsInput {
  direction?: Direction;
  filter?: TraversalFilter;
  graphName: string;
  maxHops: number;
  sourceId: string;
  targetId: string;
}

export interface QueryPathsOutput {
  paths: Path[];
}

export interface QueryVectorsInput {
  vectorBucketName?: string;
  indexName?: string;
  indexArn?: string;
  topK: number;
  queryVector: VectorData;
  filter?: unknown;
  returnMetadata?: boolean;
  returnDistance?: boolean;
  nextToken?: string;
}

export type QueryVectorsNextToken = string;

export interface QueryVectorsOutput {
  vectors: QueryOutputVector[];
  distanceMetric: DistanceMetric;
  nextToken?: string;
}

export type QueryVectorsOutputList = QueryOutputVector[];

export interface ReachedNode {
  hops: number;
  nodeId: string;
  pathConfidence: number;
}

export type ReachedNodeList = ReachedNode[];

export interface RequestTimeoutException {
  message: string;
}

export type ResourceARN = string;

export interface ServiceQuotaExceededException {
  message: string;
}

export interface ServiceUnavailableException {
  message: string;
}

export type SnapshotId = number;

export type SseType = "AES256" | "aws:kms";

export type String = string;

export interface Subgraph {
  edges: TripletReceipt[];
  nodes: ReachedNode[];
}

export type TagKey = string;

export type TagKeyList = string[];

export interface TagResourceInput {
  resourceArn: string;
  tags: Record<string, string>;
}

export interface TagResourceOutput {
}

export type TagsMap = Record<string, string>;

export type TagValue = string;

export type Timestamp = string;

export interface TooManyRequestsException {
  message: string;
}

export type TopK = number;

export interface TraversalFilter {
  minConfidence?: number;
  predicate?: string;
}

export interface TripletReceipt {
  confidence: number;
  edgeId: string;
  predicate: string;
  provenance: string;
  sourceId: string;
  targetId: string;
}

export type TripletReceiptList = TripletReceipt[];

export interface UntagResourceInput {
  resourceArn: string;
  tagKeys: string[];
}

export interface UntagResourceOutput {
}

export interface ValidationException {
  message: string;
  fieldList?: ValidationExceptionField[];
}

export interface ValidationExceptionField {
  path: string;
  message: string;
}

export type ValidationExceptionFieldList = ValidationExceptionField[];

export interface VectorBucket {
  vectorBucketName: string;
  vectorBucketArn: string;
  creationTime: string;
  encryptionConfiguration?: EncryptionConfiguration;
}

export type VectorBucketArn = string;

export type VectorBucketName = string;

export type VectorBucketPolicy = string;

export interface VectorBucketSummary {
  vectorBucketName: string;
  vectorBucketArn: string;
  creationTime: string;
}

export type VectorData = { float32: number[] };

export type VectorKey = string;
