"""Tests for many-to-many junction table population.

These tests stress-test the library's ability to handle many-to-many relationships
by populating junction/link tables through various JSON representations.

Test scenarios covered:
1. List of IDs on parent: {"id": "Q1", "choice_ids": ["c1", "c2"]}
2. Link dict: {"question_choices": {"Q1": ["c1", "c2"], "Q2": ["c2"]}}
3. Link array: [{"question_id": "Q1", "choice_id": "c1"}, ...]
4. Inverse relationship: Choices have question_ids lists
5. With DB-generated IDs: Junction records using lookups to resolve IDs
"""

from dataclasses import dataclass

import pytest

from etielle.fluent import etl, Field, TempField, node
from etielle.transforms import get, key, get_from_parent, index, lookup


# =============================================================================
# Models for dataclass-based tests (no DB)
# =============================================================================


@dataclass
class Question:
    """Question entity for M2M relationship."""

    __tablename__ = "questions"
    id: str
    text: str


@dataclass
class Choice:
    """Choice entity for M2M relationship."""

    __tablename__ = "choices"
    id: str
    text: str


@dataclass
class QuestionChoice:
    """Junction table for Question-Choice M2M relationship."""

    __tablename__ = "question_choices"
    question_id: str
    choice_id: str


# =============================================================================
# Test Scenario 1: List of IDs on Parent
# =============================================================================


class TestListOfIdsOnParent:
    """Test M2M junction table creation from list of IDs on parent.

    Data pattern:
    {
        "questions": [
            {"id": "Q1", "text": "Question 1", "choice_ids": ["c1", "c2"]},
            {"id": "Q2", "text": "Question 2", "choice_ids": ["c2", "c3"]}
        ],
        "choices": [
            {"id": "c1", "text": "Choice 1"},
            {"id": "c2", "text": "Choice 2"},
            {"id": "c3", "text": "Choice 3"}
        ]
    }
    """

    def test_junction_from_nested_ids_list(self):
        """Create junction records from choice_ids list on each question."""
        data = {
            "questions": [
                {"id": "Q1", "text": "Question 1", "choice_ids": ["c1", "c2"]},
                {"id": "Q2", "text": "Question 2", "choice_ids": ["c2", "c3"]},
            ],
            "choices": [
                {"id": "c1", "text": "Choice 1"},
                {"id": "c2", "text": "Choice 2"},
                {"id": "c3", "text": "Choice 3"},
            ],
        }

        result = (
            etl(data)
            # Extract questions
            .goto("questions")
            .each()
            .map_to(
                table=Question,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # Extract choices
            .goto_root()
            .goto("choices")
            .each()
            .map_to(
                table=Choice,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # Create junction records: iterate questions, then choice_ids
            .goto_root()
            .goto("questions")
            .each()
            .goto("choice_ids")
            .each()
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", get_from_parent("id")),
                    Field("choice_id", node()),
                ],
            )
            .run()
        )

        # Verify questions
        questions = result.tables[Question]
        assert len(questions) == 2
        question_ids = {q.id for q in questions.values()}
        assert question_ids == {"Q1", "Q2"}

        # Verify choices
        choices = result.tables[Choice]
        assert len(choices) == 3
        choice_ids = {c.id for c in choices.values()}
        assert choice_ids == {"c1", "c2", "c3"}

        # Verify junction records
        junctions = result.tables[QuestionChoice]
        assert len(junctions) == 4  # Q1->c1, Q1->c2, Q2->c2, Q2->c3

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions.values()}
        expected_pairs = {("Q1", "c1"), ("Q1", "c2"), ("Q2", "c2"), ("Q2", "c3")}
        assert junction_pairs == expected_pairs

    def test_junction_with_empty_choice_list(self):
        """Handle questions with no choices gracefully."""
        data = {
            "questions": [
                {"id": "Q1", "text": "Question 1", "choice_ids": ["c1"]},
                {"id": "Q2", "text": "Question 2", "choice_ids": []},  # Empty list
            ],
        }

        result = (
            etl(data)
            # Extract questions
            .goto("questions")
            .each()
            .map_to(
                table=Question,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # Create junction records in separate traversal
            .goto_root()
            .goto("questions")
            .each()
            .goto("choice_ids")
            .each()
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", get_from_parent("id")),
                    Field("choice_id", node()),
                ],
            )
            .run()
        )

        # Verify questions
        questions = result.tables[Question]
        assert len(questions) == 2

        # Only one junction (from Q1)
        junctions = result.tables[QuestionChoice]
        assert len(junctions) == 1
        j = list(junctions.values())[0]
        assert j.question_id == "Q1"
        assert j.choice_id == "c1"


# =============================================================================
# Test Scenario 2: Link Dict Pattern
# =============================================================================


class TestLinkDictPattern:
    """Test M2M junction table creation from link dict.

    This tests the wrapped mapping pattern where each entry is an object
    with an explicit `choices` list. For direct dict-of-lists iteration
    with `.each().each()`, see tests/test_nested_iteration.py.

    Data pattern:
    {
        "question_choices": [
            {"question_id": "Q1", "choices": ["c1", "c2"]},
            {"question_id": "Q2", "choices": ["c2", "c3"]}
        ]
    }
    """

    def test_junction_from_dict_wrapped_mapping(self):
        """Create junction records from list of {parent_id, child_ids} objects."""
        data = {
            "question_choices": [
                {"question_id": "Q1", "choices": ["c1", "c2"]},
                {"question_id": "Q2", "choices": ["c2", "c3"]},
            ]
        }

        result = (
            etl(data)
            .goto("question_choices")
            .each()  # Iterate list of mappings
            .goto("choices")
            .each()  # Iterate choices list
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", get_from_parent("question_id")),
                    Field("choice_id", node()),
                ],
            )
            .run()
        )

        junctions = result.tables[QuestionChoice]
        assert len(junctions) == 4

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions.values()}
        expected_pairs = {("Q1", "c1"), ("Q1", "c2"), ("Q2", "c2"), ("Q2", "c3")}
        assert junction_pairs == expected_pairs

    def test_junction_from_nested_dict_with_entities(self):
        """Create junctions while also extracting entities from separate paths."""
        data = {
            "questions": [
                {"id": "Q1", "text": "Question 1"},
                {"id": "Q2", "text": "Question 2"},
            ],
            "choices": [
                {"id": "c1", "text": "Choice 1"},
                {"id": "c2", "text": "Choice 2"},
            ],
            "question_choices": [
                {"question_id": "Q1", "choices": ["c1", "c2"]},
                {"question_id": "Q2", "choices": ["c2"]},
            ],
        }

        result = (
            etl(data)
            # Extract questions
            .goto("questions")
            .each()
            .map_to(
                table=Question,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # Extract choices
            .goto_root()
            .goto("choices")
            .each()
            .map_to(
                table=Choice,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # Create junction from mapping list
            .goto_root()
            .goto("question_choices")
            .each()
            .goto("choices")
            .each()
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", get_from_parent("question_id")),
                    Field("choice_id", node()),
                ],
            )
            .run()
        )

        # Verify all entities extracted
        assert len(result.tables[Question]) == 2
        assert len(result.tables[Choice]) == 2

        # Verify junctions
        junctions = result.tables[QuestionChoice]
        assert len(junctions) == 3

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions.values()}
        expected_pairs = {("Q1", "c1"), ("Q1", "c2"), ("Q2", "c2")}
        assert junction_pairs == expected_pairs


# =============================================================================
# Test Scenario 3: Link Array Pattern
# =============================================================================


class TestLinkArrayPattern:
    """Test M2M junction table creation from explicit link array.

    Data pattern:
    {
        "question_choices": [
            {"question_id": "Q1", "choice_id": "c1"},
            {"question_id": "Q1", "choice_id": "c2"},
            {"question_id": "Q2", "choice_id": "c2"}
        ]
    }
    """

    def test_junction_from_explicit_array(self):
        """Create junction records directly from array of link objects."""
        data = {
            "question_choices": [
                {"question_id": "Q1", "choice_id": "c1"},
                {"question_id": "Q1", "choice_id": "c2"},
                {"question_id": "Q2", "choice_id": "c2"},
                {"question_id": "Q2", "choice_id": "c3"},
            ]
        }

        result = (
            etl(data)
            .goto("question_choices")
            .each()
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", get("question_id")),
                    Field("choice_id", get("choice_id")),
                ],
            )
            .run()
        )

        junctions = result.tables[QuestionChoice]
        assert len(junctions) == 4

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions.values()}
        expected_pairs = {("Q1", "c1"), ("Q1", "c2"), ("Q2", "c2"), ("Q2", "c3")}
        assert junction_pairs == expected_pairs

    def test_junction_array_with_extra_fields(self):
        """Junction records can have additional metadata fields."""

        @dataclass
        class QuestionChoiceWithOrder:
            __tablename__ = "question_choices"
            question_id: str
            choice_id: str
            display_order: int

        data = {
            "question_choices": [
                {"question_id": "Q1", "choice_id": "c1", "order": 1},
                {"question_id": "Q1", "choice_id": "c2", "order": 2},
                {"question_id": "Q2", "choice_id": "c2", "order": 1},
            ]
        }

        result = (
            etl(data)
            .goto("question_choices")
            .each()
            .map_to(
                table=QuestionChoiceWithOrder,
                fields=[
                    Field("question_id", get("question_id")),
                    Field("choice_id", get("choice_id")),
                    Field("display_order", get("order")),
                ],
            )
            .run()
        )

        junctions = result.tables[QuestionChoiceWithOrder]
        assert len(junctions) == 3

        # Check a specific junction has the order field
        junction_list = list(junctions.values())
        q1_c1 = next(j for j in junction_list if j.question_id == "Q1" and j.choice_id == "c1")
        assert q1_c1.display_order == 1


# =============================================================================
# Test Scenario 4: Inverse Relationship
# =============================================================================


class TestInverseRelationship:
    """Test M2M junction where the IDs list is on the child side.

    Data pattern:
    {
        "choices": [
            {"id": "c1", "text": "Choice 1", "question_ids": ["Q1"]},
            {"id": "c2", "text": "Choice 2", "question_ids": ["Q1", "Q2"]},
            {"id": "c3", "text": "Choice 3", "question_ids": ["Q2"]}
        ]
    }
    """

    def test_junction_from_inverse_ids_list(self):
        """Create junction records when child has list of parent IDs."""
        data = {
            "choices": [
                {"id": "c1", "text": "Choice 1", "question_ids": ["Q1"]},
                {"id": "c2", "text": "Choice 2", "question_ids": ["Q1", "Q2"]},
                {"id": "c3", "text": "Choice 3", "question_ids": ["Q2"]},
            ]
        }

        result = (
            etl(data)
            # Extract choices (separate traversal)
            .goto("choices")
            .each()
            .map_to(
                table=Choice,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # Create junction: iterate choices, then question_ids (separate traversal)
            .goto_root()
            .goto("choices")
            .each()
            .goto("question_ids")
            .each()
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", node()),  # Current item from question_ids
                    Field("choice_id", get_from_parent("id")),  # Parent choice's ID
                ],
            )
            .run()
        )

        # Verify choices
        choices = result.tables[Choice]
        assert len(choices) == 3

        # Verify junction records
        junctions = result.tables[QuestionChoice]
        assert len(junctions) == 4  # c1->Q1, c2->Q1, c2->Q2, c3->Q2

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions.values()}
        expected_pairs = {("Q1", "c1"), ("Q1", "c2"), ("Q2", "c2"), ("Q2", "c3")}
        assert junction_pairs == expected_pairs


# =============================================================================
# Test Scenario 5: Junction with DB-Generated IDs (Lookup-based)
# =============================================================================


class TestJunctionWithLookup:
    """Test M2M junction table creation using lookups to resolve IDs.

    This pattern is used when:
    - Parent entities have user-facing identifiers (slugs, external IDs)
    - Junction needs DB-generated auto-increment IDs
    - Lookups resolve slug -> DB ID

    Data pattern:
    {
        "questions": [{"slug": "q1", "text": "Question 1"}, ...],
        "choices": [{"slug": "c1", "text": "Choice 1"}, ...],
        "mappings": [{"question": "q1", "choice": "c1"}, ...]
    }
    """

    def test_junction_using_index_lookup(self):
        """Junction uses lookup() to resolve external IDs to internal IDs."""
        data = {
            "questions": [
                {"slug": "q1", "text": "Question 1"},
                {"slug": "q2", "text": "Question 2"},
            ],
            "choices": [
                {"slug": "c1", "text": "Choice 1"},
                {"slug": "c2", "text": "Choice 2"},
            ],
            "mappings": [
                {"question": "q1", "choice": "c1"},
                {"question": "q1", "choice": "c2"},
                {"question": "q2", "choice": "c2"},
            ],
        }

        result = (
            etl(data)
            # Extract questions and build index slug -> question object
            .goto("questions")
            .each()
            .map_to(
                table=Question,
                fields=[
                    Field("id", get("slug")),  # Using slug as ID for simplicity
                    Field("text", get("text")),
                ],
            )
            .build_index("question_by_slug", key=get("slug"), value=get("slug"))
            # Extract choices and build index
            .goto_root()
            .goto("choices")
            .each()
            .map_to(
                table=Choice,
                fields=[
                    Field("id", get("slug")),
                    Field("text", get("text")),
                ],
            )
            .build_index("choice_by_slug", key=get("slug"), value=get("slug"))
            # Create junction using lookups
            .goto_root()
            .goto("mappings")
            .each()
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", lookup("question_by_slug", get("question"))),
                    Field("choice_id", lookup("choice_by_slug", get("choice"))),
                ],
            )
            .run()
        )

        # Verify entities
        assert len(result.tables[Question]) == 2
        assert len(result.tables[Choice]) == 2

        # Verify junction records
        junctions = result.tables[QuestionChoice]
        assert len(junctions) == 3

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions.values()}
        expected_pairs = {("q1", "c1"), ("q1", "c2"), ("q2", "c2")}
        assert junction_pairs == expected_pairs


# =============================================================================
# Additional Edge Cases
# =============================================================================


class TestManyToManyEdgeCases:
    """Edge cases and complex M2M scenarios."""

    def test_multiple_junction_tables_same_pipeline(self):
        """Create multiple different junction tables in a single pipeline."""

        @dataclass
        class Tag:
            __tablename__ = "tags"
            id: str
            name: str

        @dataclass
        class QuestionTag:
            __tablename__ = "question_tags"
            question_id: str
            tag_id: str

        data = {
            "questions": [
                {
                    "id": "Q1",
                    "text": "Question 1",
                    "choice_ids": ["c1", "c2"],
                    "tag_ids": ["t1"],
                }
            ],
            "choices": [{"id": "c1", "text": "Choice 1"}, {"id": "c2", "text": "Choice 2"}],
            "tags": [{"id": "t1", "name": "Tag 1"}],
        }

        result = (
            etl(data)
            # Extract questions
            .goto("questions")
            .each()
            .map_to(
                table=Question,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # Extract choices
            .goto_root()
            .goto("choices")
            .each()
            .map_to(
                table=Choice,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # Extract tags
            .goto_root()
            .goto("tags")
            .each()
            .map_to(
                table=Tag,
                fields=[
                    Field("id", get("id")),
                    Field("name", get("name")),
                ],
            )
            # Create question-choice junction
            .goto_root()
            .goto("questions")
            .each()
            .goto("choice_ids")
            .each()
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", get_from_parent("id")),
                    Field("choice_id", node()),
                ],
            )
            # Create question-tag junction
            .goto_root()
            .goto("questions")
            .each()
            .goto("tag_ids")
            .each()
            .map_to(
                table=QuestionTag,
                fields=[
                    Field("question_id", get_from_parent("id")),
                    Field("tag_id", node()),
                ],
            )
            .run()
        )

        # Verify all tables created
        assert len(result.tables[Question]) == 1
        assert len(result.tables[Choice]) == 2
        assert len(result.tables[Tag]) == 1
        assert len(result.tables[QuestionChoice]) == 2
        assert len(result.tables[QuestionTag]) == 1

        # Verify question-choice junctions
        qc_pairs = {(j.question_id, j.choice_id) for j in result.tables[QuestionChoice].values()}
        assert qc_pairs == {("Q1", "c1"), ("Q1", "c2")}

        # Verify question-tag junctions
        qt_pairs = {(j.question_id, j.tag_id) for j in result.tables[QuestionTag].values()}
        assert qt_pairs == {("Q1", "t1")}

    def test_self_referential_junction(self):
        """Junction table for self-referential M2M (e.g., related questions)."""

        @dataclass
        class RelatedQuestion:
            __tablename__ = "related_questions"
            question_id: str
            related_id: str

        data = {
            "questions": [
                {"id": "Q1", "text": "Question 1", "related_ids": ["Q2"]},
                {"id": "Q2", "text": "Question 2", "related_ids": ["Q1", "Q3"]},
                {"id": "Q3", "text": "Question 3", "related_ids": []},
            ]
        }

        result = (
            etl(data)
            # Extract questions (separate traversal)
            .goto("questions")
            .each()
            .map_to(
                table=Question,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # Create junctions (separate traversal)
            .goto_root()
            .goto("questions")
            .each()
            .goto("related_ids")
            .each()
            .map_to(
                table=RelatedQuestion,
                fields=[
                    Field("question_id", get_from_parent("id")),
                    Field("related_id", node()),
                ],
            )
            .run()
        )

        # Verify questions
        assert len(result.tables[Question]) == 3

        # Verify self-referential junctions
        junctions = result.tables[RelatedQuestion]
        assert len(junctions) == 3  # Q1->Q2, Q2->Q1, Q2->Q3

        junction_pairs = {(j.question_id, j.related_id) for j in junctions.values()}
        expected_pairs = {("Q1", "Q2"), ("Q2", "Q1"), ("Q2", "Q3")}
        assert junction_pairs == expected_pairs

    def test_deeply_nested_junction_creation(self):
        """Create junctions from nested data structure.

        NOTE: The library currently supports up to 2 levels of .each() nesting.
        For deeper structures, flatten the data or use separate traversals.
        This test uses a 2-level nested structure: questions -> choice_ids.
        """
        data = {
            "questions": [
                {"id": "Q1", "text": "Question 1", "choice_ids": ["c1", "c2"]},
                {"id": "Q2", "text": "Question 2", "choice_ids": ["c3"]},
            ]
        }

        result = (
            etl(data)
            # Extract questions (separate traversal)
            .goto("questions")
            .each()
            .map_to(
                table=Question,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # Create junctions (separate traversal)
            .goto_root()
            .goto("questions")
            .each()
            .goto("choice_ids")
            .each()
            .map_to(
                table=QuestionChoice,
                fields=[
                    Field("question_id", get_from_parent("id")),
                    Field("choice_id", node()),
                ],
            )
            .run()
        )

        # Verify questions from nested structure
        assert len(result.tables[Question]) == 2

        # Verify junctions
        junctions = result.tables[QuestionChoice]
        assert len(junctions) == 3

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions.values()}
        expected_pairs = {("Q1", "c1"), ("Q1", "c2"), ("Q2", "c3")}
        assert junction_pairs == expected_pairs


# =============================================================================
# SQLAlchemy/SQLModel Adapter Tests
# =============================================================================


class TestSQLAlchemyManyToMany:
    """Test M2M junction table population with SQLAlchemy ORM.

    These tests verify that junction tables are correctly persisted to a database
    when using SQLAlchemy models and sessions.
    """

    @pytest.fixture
    def engine_and_session(self):
        """Create in-memory SQLite database with ORM models."""
        from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
        from sqlalchemy.orm import declarative_base, relationship, sessionmaker

        Base = declarative_base()

        class QuestionORM(Base):
            __tablename__ = "questions"
            id = Column(String, primary_key=True)
            text = Column(String)

        class ChoiceORM(Base):
            __tablename__ = "choices"
            id = Column(String, primary_key=True)
            text = Column(String)

        class QuestionChoiceORM(Base):
            __tablename__ = "question_choices"
            id = Column(Integer, primary_key=True, autoincrement=True)
            question_id = Column(String, ForeignKey("questions.id"), nullable=False)
            choice_id = Column(String, ForeignKey("choices.id"), nullable=False)
            question = relationship("QuestionORM")
            choice = relationship("ChoiceORM")

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        yield {
            "engine": engine,
            "session": session,
            "Question": QuestionORM,
            "Choice": ChoiceORM,
            "QuestionChoice": QuestionChoiceORM,
        }

        session.close()

    def test_junction_persisted_to_database(self, engine_and_session):
        """Junction records are correctly inserted into the database."""
        session = engine_and_session["session"]
        QuestionORM = engine_and_session["Question"]
        ChoiceORM = engine_and_session["Choice"]
        QuestionChoiceORM = engine_and_session["QuestionChoice"]

        data = {
            "questions": [
                {"id": "Q1", "text": "Question 1"},
                {"id": "Q2", "text": "Question 2"},
            ],
            "choices": [
                {"id": "c1", "text": "Choice 1"},
                {"id": "c2", "text": "Choice 2"},
            ],
            "mappings": [
                {"question_id": "Q1", "choice_id": "c1"},
                {"question_id": "Q1", "choice_id": "c2"},
                {"question_id": "Q2", "choice_id": "c2"},
            ],
        }

        (
            etl(data)
            .goto("questions")
            .each()
            .map_to(
                table=QuestionORM,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            .goto_root()
            .goto("choices")
            .each()
            .map_to(
                table=ChoiceORM,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            .goto_root()
            .goto("mappings")
            .each()
            .map_to(
                table=QuestionChoiceORM,
                fields=[
                    Field("question_id", get("question_id")),
                    Field("choice_id", get("choice_id")),
                ],
            )
            .load(session)
            .run()
        )

        session.commit()

        # Verify questions in database
        questions = session.query(QuestionORM).all()
        assert len(questions) == 2
        assert {q.id for q in questions} == {"Q1", "Q2"}

        # Verify choices in database
        choices = session.query(ChoiceORM).all()
        assert len(choices) == 2
        assert {c.id for c in choices} == {"c1", "c2"}

        # Verify junction records
        junctions = session.query(QuestionChoiceORM).all()
        assert len(junctions) == 3

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions}
        expected_pairs = {("Q1", "c1"), ("Q1", "c2"), ("Q2", "c2")}
        assert junction_pairs == expected_pairs

    def test_junction_with_link_to_binding(self, engine_and_session):
        """Junction table can use link_to() for relationship binding."""
        session = engine_and_session["session"]
        QuestionORM = engine_and_session["Question"]
        ChoiceORM = engine_and_session["Choice"]
        QuestionChoiceORM = engine_and_session["QuestionChoice"]

        data = {
            "questions": [
                {"id": "Q1", "text": "Question 1"},
            ],
            "choices": [
                {"id": "c1", "text": "Choice 1"},
                {"id": "c2", "text": "Choice 2"},
            ],
            "mappings": [
                {"question_id": "Q1", "choice_id": "c1"},
                {"question_id": "Q1", "choice_id": "c2"},
            ],
        }

        (
            etl(data)
            .goto("questions")
            .each()
            .map_to(
                table=QuestionORM,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            .build_index("question_by_id", key=get("id"), value=get("id"))
            .goto_root()
            .goto("choices")
            .each()
            .map_to(
                table=ChoiceORM,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            .build_index("choice_by_id", key=get("id"), value=get("id"))
            .goto_root()
            .goto("mappings")
            .each()
            .map_to(
                table=QuestionChoiceORM,
                fields=[
                    Field("question_id", get("question_id")),
                    Field("choice_id", get("choice_id")),
                    TempField("q_id", get("question_id")),
                    TempField("c_id", get("choice_id")),
                ],
            )
            .link_to(QuestionORM, by={"q_id": "id"})
            .link_to(ChoiceORM, by={"c_id": "id"})
            .load(session)
            .run()
        )

        session.commit()

        # Verify junction records have proper relationships
        junctions = session.query(QuestionChoiceORM).all()
        assert len(junctions) == 2

        for j in junctions:
            assert j.question is not None
            assert j.choice is not None
            assert j.question.id == j.question_id
            assert j.choice.id == j.choice_id

    def test_junction_from_nested_iteration(self, engine_and_session):
        """Junction created via nested iteration is persisted correctly."""
        session = engine_and_session["session"]
        QuestionORM = engine_and_session["Question"]
        QuestionChoiceORM = engine_and_session["QuestionChoice"]

        data = {
            "questions": [
                {"id": "Q1", "text": "Question 1", "choice_ids": ["c1", "c2"]},
                {"id": "Q2", "text": "Question 2", "choice_ids": ["c2"]},
            ],
        }

        (
            etl(data)
            .goto("questions")
            .each()
            .map_to(
                table=QuestionORM,
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            .goto_root()
            .goto("questions")
            .each()
            .goto("choice_ids")
            .each()
            .map_to(
                table=QuestionChoiceORM,
                fields=[
                    Field("question_id", get_from_parent("id")),
                    Field("choice_id", node()),
                ],
            )
            .load(session)
            .run()
        )

        session.commit()

        # Verify questions
        questions = session.query(QuestionORM).all()
        assert len(questions) == 2

        # Verify junction records
        junctions = session.query(QuestionChoiceORM).all()
        assert len(junctions) == 3

        junction_pairs = {(j.question_id, j.choice_id) for j in junctions}
        expected_pairs = {("Q1", "c1"), ("Q1", "c2"), ("Q2", "c2")}
        assert junction_pairs == expected_pairs


# =============================================================================
# Supabase Adapter Tests (Mock-based)
# =============================================================================


class TestSupabaseManyToMany:
    """Test M2M junction table population with Supabase adapter.

    These tests use mocked Supabase clients to verify that the adapter
    correctly inserts junction table records.
    """

    @pytest.fixture
    def mock_supabase_client(self):
        """Create a mock Supabase client for testing."""
        from unittest.mock import MagicMock

        mock = MagicMock()
        # Make it look like a Supabase client for type detection
        mock.__class__.__module__ = "supabase._sync.client"
        mock.__class__.__name__ = "SyncClient"

        # Track inserted data per table
        inserted_data: dict = {}

        def make_table_mock(table_name):
            table_mock = MagicMock()
            inserted_data.setdefault(table_name, [])

            def capture_insert(data):
                insert_mock = MagicMock()
                # Capture the data being inserted
                if isinstance(data, list):
                    inserted_data[table_name].extend(data)
                else:
                    inserted_data[table_name].append(data)
                insert_mock.execute.return_value.data = data if isinstance(data, list) else [data]
                return insert_mock

            table_mock.insert.side_effect = capture_insert
            return table_mock

        mock.table.side_effect = make_table_mock
        mock._inserted_data = inserted_data
        return mock

    def test_junction_table_insert_to_supabase(self, mock_supabase_client):
        """Junction records are inserted to Supabase via the adapter."""
        data = {
            "questions": [
                {"id": "Q1", "text": "Question 1"},
            ],
            "choices": [
                {"id": "c1", "text": "Choice 1"},
                {"id": "c2", "text": "Choice 2"},
            ],
            "question_choices": [
                {"question_id": "Q1", "choice_id": "c1"},
                {"question_id": "Q1", "choice_id": "c2"},
            ],
        }

        result = (
            etl(data)
            .goto("questions")
            .each()
            .map_to(
                table="questions",
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            .goto_root()
            .goto("choices")
            .each()
            .map_to(
                table="choices",
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            .goto_root()
            .goto("question_choices")
            .each()
            .map_to(
                table="question_choices",
                fields=[
                    Field("question_id", get("question_id")),
                    Field("choice_id", get("choice_id")),
                ],
            )
            .load(mock_supabase_client)
            .run()
        )

        # Verify data was captured for all tables
        inserted_data = mock_supabase_client._inserted_data

        assert "questions" in inserted_data
        assert len(inserted_data["questions"]) == 1

        assert "choices" in inserted_data
        assert len(inserted_data["choices"]) == 2

        assert "question_choices" in inserted_data
        assert len(inserted_data["question_choices"]) == 2

        # Verify junction content
        junction_pairs = {
            (j["question_id"], j["choice_id"]) for j in inserted_data["question_choices"]
        }
        expected_pairs = {("Q1", "c1"), ("Q1", "c2")}
        assert junction_pairs == expected_pairs

    def test_junction_from_nested_iteration_to_supabase(self, mock_supabase_client):
        """Junction created via nested iteration is sent to Supabase."""
        data = {
            "questions": [
                {"id": "Q1", "text": "Question 1", "choice_ids": ["c1", "c2"]},
                {"id": "Q2", "text": "Question 2", "choice_ids": ["c2"]},
            ],
        }

        result = (
            etl(data)
            .goto("questions")
            .each()
            .map_to(
                table="questions",
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            .goto_root()
            .goto("questions")
            .each()
            .goto("choice_ids")
            .each()
            .map_to(
                table="question_choices",
                fields=[
                    Field("question_id", get_from_parent("id")),
                    Field("choice_id", node()),
                ],
            )
            .load(mock_supabase_client)
            .run()
        )

        # Verify data was captured
        inserted_data = mock_supabase_client._inserted_data

        assert "questions" in inserted_data
        assert len(inserted_data["questions"]) == 2

        assert "question_choices" in inserted_data
        assert len(inserted_data["question_choices"]) == 3

        # Verify junction content
        junction_pairs = {
            (j["question_id"], j["choice_id"]) for j in inserted_data["question_choices"]
        }
        expected_pairs = {("Q1", "c1"), ("Q1", "c2"), ("Q2", "c2")}
        assert junction_pairs == expected_pairs

    def test_multiple_junction_tables_to_supabase(self, mock_supabase_client):
        """Multiple junction tables are inserted correctly."""

        data = {
            "questions": [
                {
                    "id": "Q1",
                    "text": "Question 1",
                    "choice_ids": ["c1", "c2"],
                    "tag_ids": ["t1"],
                }
            ],
        }

        result = (
            etl(data)
            # Extract questions
            .goto("questions")
            .each()
            .map_to(
                table="questions",
                fields=[
                    Field("id", get("id")),
                    Field("text", get("text")),
                ],
            )
            # Create question-choice junction
            .goto_root()
            .goto("questions")
            .each()
            .goto("choice_ids")
            .each()
            .map_to(
                table="question_choices",
                fields=[
                    Field("question_id", get_from_parent("id")),
                    Field("choice_id", node()),
                ],
            )
            # Create question-tag junction
            .goto_root()
            .goto("questions")
            .each()
            .goto("tag_ids")
            .each()
            .map_to(
                table="question_tags",
                fields=[
                    Field("question_id", get_from_parent("id")),
                    Field("tag_id", node()),
                ],
            )
            .load(mock_supabase_client)
            .run()
        )

        # Verify data was captured for all tables
        inserted_data = mock_supabase_client._inserted_data

        assert "questions" in inserted_data
        assert len(inserted_data["questions"]) == 1

        assert "question_choices" in inserted_data
        assert len(inserted_data["question_choices"]) == 2

        assert "question_tags" in inserted_data
        assert len(inserted_data["question_tags"]) == 1

        # Verify junction content
        qc_pairs = {
            (j["question_id"], j["choice_id"]) for j in inserted_data["question_choices"]
        }
        assert qc_pairs == {("Q1", "c1"), ("Q1", "c2")}

        qt_pairs = {
            (j["question_id"], j["tag_id"]) for j in inserted_data["question_tags"]
        }
        assert qt_pairs == {("Q1", "t1")}
