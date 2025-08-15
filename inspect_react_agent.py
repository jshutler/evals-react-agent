# Unified Database Agent - Works with both Anthropic models and Inspect evaluation

from typing import Any, Dict, List, Optional
from uuid import uuid4
import json
import os
from langchain_core.messages import AIMessage, convert_to_messages
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langgraph.checkpoint.memory import MemorySaver
from langgraph.prebuilt import create_react_agent
from langchain.tools import Tool
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from models import FinalReport, DAOGetAllTables, DAOGetSchemaForTable, DAORunSQL
from sqlalchemy_utils.sales_dao import SalesDAO
from db_constants import SALES_DB_CONNECTION_STRING
from datetime import datetime
load_dotenv()

REACT_AGENT_PROMPT = """You are a database agent. Your job is to answer the user's question using the tools provided
to query the database. Make sure to always check for the schema of the tables before querying the database directly"""

def log_messages_to_json(messages: list, filename: str):
    log_messages = [dict(m) for m in messages]
    with open(filename, 'w') as f:
        json.dump(log_messages, f, indent=2, ensure_ascii=False)

def create_database_tools(dao: SalesDAO) -> List[Tool]:
    """Create LangChain tools from your Pydantic models"""
    
    def get_all_tables(_input: str = ""):
        """Get all table names from the database"""
        model = DAOGetAllTables()
        return model(dao)
    
    def get_schema_for_table(table_name: str):
        """Get schema information for a specific table"""
        model = DAOGetSchemaForTable(table_name=table_name)
        return model(dao)
    
    def run_sql(query: str, params: Dict[str, Any] | None = None):
        """Execute a SQL query and return results"""
        model = DAORunSQL(query=query, params=params)
        return model(dao)
    
    def final_report(summary: str, recommendations: str = ""):
        """Generate the final report with findings and recommendations"""
        model = FinalReport(summary=summary, recommendations=recommendations)
        return model()
    
    # Convert to LangChain Tools
    tools = [
        Tool(
            name="get_all_tables",
            description="Get all table names from the database",
            func=get_all_tables
        ),
        Tool(
            name="get_schema_for_table", 
            description="Get schema information for a specific table. Input should be the table name.",
            func=get_schema_for_table
        ),
        Tool(
            name="run_sql",
            description="Execute a SQL query and return results. Input should be a valid SQL query string.",
            func=run_sql
        ),
        Tool(
            name="final_report",
            description="Generate the final report with findings and recommendations. Input should be a summary string.",
            func=final_report
        )
    ]
    
    return tools

def db_agent(*, react_agent_prompt: str, use_anthropic: bool = True, model_name: str = None):
    """Database analysis agent that works with both Anthropic and Inspect evaluation.
    
    Args:
        use_anthropic: If True, use Anthropic model. If False, use OpenAI interface (for Inspect)
        model_name: Specific model name to use
        
    Returns:
        Agent function for handling samples. May be passed to Inspect `bridge()`
        to create a standard Inspect solver.
    """
    
    # Initialize DAO
    dao = SalesDAO(SALES_DB_CONNECTION_STRING)
    
    # Create tools
    tools = create_database_tools(dao)
    
    # Choose model based on context
    if use_anthropic:
        # Production mode with Anthropic
        if model_name is None:
            model_name = "claude-3-haiku-20240307"
        model = ChatAnthropic(
            model=model_name,
            api_key=os.environ["ANTHROPIC_API_KEY"]
        )
    else:
        # Evaluation mode with Inspect (uses OpenAI interface redirected to Inspect)
        model = ChatOpenAI(model="inspect")
    
    # Create the LangGraph agent
    executor = create_react_agent(
        model=model,
        tools=tools,
        checkpointer=MemorySaver(),
        prompt=react_agent_prompt
    )
    
    # Sample handler (works for both production and evaluation)
    async def run(sample: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        # Handle different input formats
        if isinstance(sample, dict) and "input" in sample:
            # Inspect evaluation format
            input_messages = convert_to_messages(sample["input"])
        else:
            # Direct usage format
            input_messages = convert_to_messages([{"role": "user", "content": str(sample)}])
        
        # Execute the agent
        result = await executor.ainvoke(
            input={"messages": input_messages},
            config={"configurable": {"thread_id": str(uuid4())}},
        )
        log_filename = f"logs/agent_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        log_messages_to_json(result["messages"], log_filename)
        
        # Return output (content of last message)
        message: AIMessage = result["messages"][-1]
        return dict(output=str(message.content))
    
    return run

# For direct production usage
async def query_database_agent(query: str, use_anthropic: bool = True, model_name: str = None):
    """Direct interface for production usage"""
    agent = db_agent(react_agent_prompt=REACT_AGENT_PROMPT, use_anthropic=use_anthropic, model_name=model_name)
    final_message = await agent(query)
    return final_message

# For Inspect evaluation usage
def db_agent_for_inspect():
    """Agent configured specifically for Inspect evaluation"""
    return db_agent(use_anthropic=False)

# Example usage patterns:

if __name__ == "__main__":
    import asyncio
    dao = SalesDAO(SALES_DB_CONNECTION_STRING)
    
    tools = create_database_tools(dao)
    # # Production usage with Anthropic
    async def production_example():
        query = "What is the total value of all sales in the database as of right now? And how many total transactions have been made?"
        result = await query_database_agent(query, use_anthropic=True)
        print(f"Production result: {result}")
    
    # # Run production example
    asyncio.run(production_example())
