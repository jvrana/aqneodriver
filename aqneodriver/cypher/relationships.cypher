MATCH (a:Sample)
MATCH (b:SampleType)
WHERE a.sample_type_id = b.id
CREATE (a) -[r:hasMetaType]-> (b)
RETURN a, r, b

MATCH (a:Item)
MATCH (b:ObjectType)
WHERE a.object_type = b.id
CREATE (a) -[r:hasMetaType]-> (b)
RETURN a, r, b

MATCH (a:FieldValue)
MATCH (b:FieldType)
WHERE a.field_type_id = b.id
CREATE (a) -[r:hasMetaType]-> (b)
RETURN a, r, b

MATCH (a:Operation)
MATCH (b:OperationType)
WHERE a.field_type_id = b.id
CREATE (a) -[r:hasMetaType]-> (b)
RETURN a, r, b

MATCH (a:SampleType)
MATCH (b:FieldType { parent_class: "SampleType" })
WHERE a.id = b.parent_id
CREATE (a) -[r:hasFieldType]-> (b)
RETURN a, r, b

MATCH (a:OperationType)
MATCH (b:FieldType { parent_class: "OperationType" })
WHERE a.id = b.parent_id
CREATE (a) -[r:hasFieldType]-> (b)
RETURN a, r, b

MATCH (a:OperationType)
MATCH (b:FieldType { parent_class: "OperationType", role: "input" })
WHERE a.id = b.parent_id
CREATE (a) -[r:hasInput]-> (b) -[r:inputsTo] -> (a)
RETURN a, r, b

MATCH (a:OperationType)
MATCH (b:FieldType { parent_class: "OperationType", role: "output" })
WHERE a.id = b.parent_id
CREATE (a) -[r:hasOutput]-> (b) <-[r:outputsFrom]- (a)
RETURN a, r, b

MATCH (a:FieldValue { parent_class: "Sample"})
MATCH (b:Sample)
WHERE a.parent_class = "Sample" AND a.child_sample_id = b.id
CREATE (a) -[r:hasSampleProperty]-> (b)
RETURN a, r, b

MATCH (a:FieldValue { parent_class: "Operation" })
MATCH (b:Item)
WHERE a.child_item_id = b.id
CREATE (a) -[r:hasItem]-> (b)
RETURN a, r, b

MATCH (a:Sample)
MATCH (b:Item)
WHERE b.child_sample_id = a.id
CREATE (a) -[r:hasItem]-> (b)
RETURN a, r, b

MATCH (a:FieldValue { parent_class: "Sample" })
MATCH (a:FieldValue) -[:hasSample] -> (:Sample) -[:hasMetaType] -> (:SampleType) -[:hasFieldType]-> (b:FieldType)
CREATE (a) -[r:maybeMetaType]-> (b)
RETURN a, r, b

MATCH (op:Operation)
MATCH (ja:JobAssociation)
MATCH (job:Job)
WHERE op.id = ja.operation_id AND ja.job_id = job.id
CREATE (op) <-[:associatedWith]-> (ja) <-[:associatedWith]-> (job)

MATCH (op:Operation)
MATCH (pa:PlanAssociation)
MATCH (plan:Plan)
WHERE op.id = pa.operation_id AND pa.plan_id = plan.id
CREATE (op) <-[:associatedWith]-> (pa) <-[:associatedWith]-> (plan)

// TODO: add allowable field types and values
