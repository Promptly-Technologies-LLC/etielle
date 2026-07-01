## ConstructorBuilder


Simplified builder for classes that accept keyword arguments in their constructor.


Usage

``` python
ConstructorBuilder()
```


Perfect for SQLAlchemy/SQLModel ORM models.


## Usage

builder = ConstructorBuilder(User) \# Just pass the class


## Equivalent To

builder = TypedDictBuilder(lambda d: User(\*\*d))
