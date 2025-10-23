from etielle.core import (
    MappingSpec,
    TraversalSpec,
    TableEmit,
    Field
)
from etielle.transforms import (
    key,
    get,
    get_from_root,
    get_from_parent,
    len_of,
    format_id,
    parent_key,
    index as iter_index,
)
from etielle.executor import run_mapping


def _bool_from_text_entry(ctx):
    """Coerce Qualtrics textEntry config into a boolean flag.

    Treat any non-null value (including an empty object {}) as True.
    """
    val = get("textEntry")(ctx)
    return bool(val is not None)


def build_definition_mapping_spec() -> MappingSpec:
    """
    Build a MappingSpec equivalent to utils.app.constants.table_mappings
    for survey definition JSON → tables: surveys, blocks, questions, subquestions, choices.
    """
    surveys_emit = TraversalSpec(
        path=[],
        mode="single",
        emits=[
            TableEmit(
                table="surveys",
                join_keys=[get("id")],
                fields=[
                    Field("id", get("id")),
                    Field("name", get("name")),
                    Field("is_active", get("isActive")),
                    Field("owner", get("ownerId")),
                    Field("organization", get("organizationId")),
                    Field("auditable", get("responseCounts.auditable")),
                    Field("deleted", get("responseCounts.deleted")),
                    Field("generated", get("responseCounts.generated")),
                    Field("end_date", get("expiration.endDate")),
                    Field("start_date", get("expiration.startDate")),
                    Field("embedded_data_definition", get("embeddedData")),
                ],
            )
        ],
    )

    blocks_emit = TraversalSpec(
        path=["blocks"],
        mode="items",
        emits=[
            TableEmit(
                table="blocks",
                join_keys=[key()],
                fields=[
                    Field("qualtrics_id", key()),
                    Field("elements", len_of(get("elements"))),
                    Field("description", get("description"))
                ],
            )
        ],
    )

    questions_from_questions_emit = TraversalSpec(
        path=["questions"],
        mode="items",
        emits=[
            TableEmit(
                table="questions",
                join_keys=[key()],
                fields=[
                    Field("qualtrics_id", key()),
                    Field("question_name", get("questionName")),
                    Field("question_text", get("questionText")),
                    Field("type", get("questionType.type")),
                    Field("selector", get("questionType.selector")),
                    Field("sub_selector", get("questionType.subSelector"))
                ],
            )
        ],
    )

    questions_from_blocks_elements_emit = TraversalSpec(
        path=["blocks"],
        mode="items",
        inner_path=["elements"],
        inner_mode="single",
        emits=[
            TableEmit(
                table="questions",
                join_keys=[get("questionId")],
                fields=[
                    Field("qualtrics_id", get("questionId"))
                ],
            )
        ],
    )

    subquestions_emit = TraversalSpec(
        path=["questions"],
        mode="items",
        inner_path=["subQuestions"],
        inner_mode="items",
        emits=[
            TableEmit(
                table="subquestions",
                join_keys=[parent_key(), key()],
                fields=[
                    Field("qualtrics_id", format_id(parent_key(), key())),
                    Field("subquestion_text", get("choiceText")),
                    Field("subquestion_description", get("description")),
                    Field("subquestion_recode", get("recode")),
                    Field("variable_name", get("variableName")),
                    Field("allows_text_entry", _bool_from_text_entry()),
                ],
            )
        ],
    )

    choices_emit = TraversalSpec(
        path=["questions"],
        mode="items",
        inner_path=["choices"],
        inner_mode="items",
        emits=[
            TableEmit(
                table="choices",
                join_keys=[parent_key(), key()],
                fields=[
                    Field("qualtrics_id", format_id(parent_key(), key())),
                    Field("choice_recode", get("recode")),
                    Field("choice_description", get("description")),
                    Field("choice_text", get("choiceText")),
                    Field("variable_name", get("variableName")),
                    Field("allows_text_entry", _bool_from_text_entry()),
                ],
            )
        ],
    )

    return MappingSpec(
        traversals=[
            surveys_emit,
            blocks_emit,
            questions_from_questions_emit,
            questions_from_blocks_elements_emit,
            subquestions_emit,
            choices_emit,
        ]
    )


def map_definition_with_dsl(schema: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Run the definition mapping DSL against a schema and return table rows."""
    spec = build_definition_mapping_spec()
    return run_mapping(schema, spec)
