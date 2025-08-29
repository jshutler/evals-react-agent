from inspect_ai import Task, task
from inspect_ai.dataset import json_dataset
from inspect_ai.scorer import model_graded_fact
from inspect_ai.solver import bridge
from inspect_ai.model import get_model
from inspect_react_agent import db_agent, REACT_AGENT_PROMPT

@task
def db_testing():
    return Task(
        dataset=json_dataset("db_testing.json"),
        solver=[bridge(db_agent(react_agent_prompt=REACT_AGENT_PROMPT, use_anthropic=False))],
        scorer=model_graded_fact(model="anthropic/claude-3-haiku-20240307"),  # Use GPT-4 for scoring
        model="anthropic/claude-3-haiku-20240307"  # Use Claude Sonnet for the solver
    )