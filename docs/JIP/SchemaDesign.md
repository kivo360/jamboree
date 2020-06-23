```py
class RequirementsSchema(Schema):
    __metadata__ = DescriptionObject(**parameters)

    field = FieldType(name,**parameters) # parameters here describe how the data will be used
    field = FieldType(**parameters)
    field = FieldType(**parameters)
    field = FieldType(**parameters)
    field = FieldType(**parameters)
```