MATCH (a:Sample)
MATCH (b:SampleType)
WHERE a.sample_type_id = b.id
MERGE (a) -[r:hasMetaType]-> (b)
RETURN a, r, b

MATCH (a:Sample)
MATCH (b:SampleType)
WHERE a.sample_type_id = b.id
MERGE (a) -[r:hasMetaType]-> (b)
RETURN a, r, b

MATCH (a:FieldValue)
MATCH (b:FieldType)
WHERE a.field_type_id = b.id
MERGE (a) -[r:hasMetaType]-> (b)
RETURN a, r, b

MATCH (a:SampleType)
MATCH (b:FieldType { parent_class: "SampleType" })
WHERE a.id = b.parent_id
MERGE (a) -[r:hasFieldType]-> (b)
RETURN a, r, b

MATCH (a:FieldValue { parent_class: "Sample"})
MATCH (b:Sample)
WHERE a.child_sample_id = b.id
MERGE (a) -[r:hasSampleProperty]-> (b)
RETURN a, r, b

MATCH (a:Sample)
MATCH (b:FieldValue { parent_class: "Sample" })
WHERE a.id = b.parent_id
MERGE (a) -[r:hasFieldValue]-> (b)
RETURN a, r, b

MATCH (a:Item)
MATCH (b:ObjectType)
WHERE a.object_type_id = b.id
MERGE (a) -[r:hasMetaType]-> (b)
RETURN a, r, b

MATCH (a:Item)
MATCH (b:Sample)
WHERE a.sample_id = b.id
MERGE (a) -[r:memberOf]-> (b)
RETURN a, r, r2, b

MATCH (a:FieldValue { parent_class: "Sample"}) -[:hasSample]-> (:Sample) -[:hasMetaType]-> (:SampleType) -[:hasFieldType]-> (b:FieldType)
WHERE a.name = b.name
MERGE (a) -[r:maybeMetaType]-> (b)
RETURN a, r, b

MATCH (a:Operation)
MATCH (b:OperationType)
WHERE a.operation_type_id = b.id
MERGE (a) -[r:hasMetaType]-> (b)
RETURN a, r, b

MATCH (a:OperationType)
MATCH (b:FieldType { parent_class: "OperationType" })
WHERE a.id = b.parent_id
MERGE (a) -[r:hasFieldType]-> (b)
RETURN a, r, b

//MATCH (a:OperationType)
//MATCH (b:FieldType { parent_class: "OperationType", role: "input" })
//WHERE a.id = b.parent_id
//MERGE (a) -[r1:hasInput]-> (b) -[r2:inputsTo] -> (a)
//RETURN a, r1, r2, b

//MATCH (a:OperationType)
//MATCH (b:FieldType { parent_class: "OperationType", role: "output" })
//WHERE a.id = b.parent_id
//MERGE (a) -[r1:hasOutput]-> (b) <-[r2:outputsFrom]- (a)
//RETURN a, r1, r2, b

//MATCH (a:Operation)
//MATCH (b:FieldValue { parent_class: "Operation", role: "input" })
//WHERE a.id = b.parent_id
//MERGE (a) -[r1:hasInput]-> (b) -[r2:inputsTo] -> (a)
//RETURN a, r1, r2, b

//MATCH (a:Operation)
//MATCH (b:FieldValue { parent_class: "Operation", role: "output" })
//WHERE a.id = b.parent_id
//MERGE (a) -[r1:hasOutput]-> (b) <-[r2:outputsFrom]- (a)
//RETURN a, r1, r2, b

MATCH (a:FieldValue { parent_class: "Operation" })
MATCH (b:Item)
WHERE a.child_item_id = b.id
MERGE (a) -[r:hasItem]-> (b)
RETURN a, r, b

MATCH (a:Sample)
MATCH (b:Item)
WHERE b.child_sample_id = a.id
MERGE (a) -[r:hasItem]-> (b)
RETURN a, r, b

MATCH (op:Operation)
MATCH (pa:JobAssociation)
WHERE op.id = pa.operation_id
MERGE (p:Job { id: pa.job_id })
MERGE (op) <-[r1:associatedWith]-> (pa) <-[r2:associatedWith]-> (p)
RETURN op, pa, p, r1, r2

MATCH (op:Operation)
MATCH (pa:PlanAssociation)
WHERE op.id = pa.operation_id
MERGE (p:Plan { id: pa.plan_id })
MERGE (op) <-[r1:associatedWith]-> (pa) <-[r2:associatedWith]-> (p)
RETURN op, pa, p, r1, r2
// TODO: add allowable field types and values

MATCH (ft1:FieldType { parent_class: "OperationType", role: "input" })
MATCH (ft2:FieldType { parent_class: "OperationType", role: "output" })
WHERE ft1.parent_id = ft2.parent_id AND ft1.routing = ft2.routing
MERGE (ft1) -[r:routedTo { routing: ft1.routing }]-> (ft2)
RETURN ft1, r, ft2

MATCH (fv1:FieldValue) -[]-> (ft1:FieldType) -[:routedTo]-> (ft2:FieldType) <-[]- (fv2:FieldValue)
MERGE (fv1) -[r:routedTo { routing: ft1.routing }]-> (fv2)
RETURN fv1, r, fv2


MATCH (fv:FieldValue {parent_class: "Sample"}) -[:hasMetaType]-> (ft:FieldType)
WHERE ft.ftype <> 'sample'
MERGE (prop:Property {ftype: ft.ftype, value: fv.value, name: fv.name, sample_type_id: ft.parent_id})
MERGE (prop) <-[r:hasProperty]- (fv)
RETURN prop, r, fv