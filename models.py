from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional


class FinalReport(BaseModel):
    """Schema for the final report that must be the last task in the plan. Should summarize findings and recommendations."""
    summary: str = Field(..., description="Summary of findings and actions taken")
    task_completed: bool = Field(True, description="Indicates that all tasks have been completed")
    
    def __call__(self):
        """Returns a formatted final report."""
        report = {
            "summary": self.summary
        }
        return report
    

class DAOGetAllTables(BaseModel):
    """
    Get all tables in the database
    """
    def __call__(self, dao):
        return dao.get_all_tables()
    
class DAOGetSchemaForTable(BaseModel):
    """
    Get schema for a specific table
    """
    table_name: str
    
    def __call__(self, dao):
        return dao.get_schema_for_table(self.table_name)
    
class DAORunSQL(BaseModel):
    """
    Run arbitrary SQL query
    """
    query: str
    params: Optional[Dict[str, Any]] = None

    def __call__(self, dao):
        return dao.run_sql(self.query, self.params)


class PythonCodeExecutor(BaseModel):
    """
    Execute arbitrary Python code
    """
    code: str = Field(..., description="Python code to execute")
    globals_dict: Optional[Dict[str, Any]] = Field(default=None, description="Global variables to make available to the code")
    locals_dict: Optional[Dict[str, Any]] = Field(default=None, description="Local variables to make available to the code")

    def __call__(self, **kwargs):
        """
        Execute the Python code and return the result.
        The code can access any variables passed in kwargs.
        """
        # Create execution environment
        exec_globals = globals().copy()
        if self.globals_dict:
            exec_globals.update(self.globals_dict)
        
        exec_locals = locals().copy()
        if self.locals_dict:
            exec_locals.update(self.locals_dict)
        
        # Add kwargs to locals for code access
        exec_locals.update(kwargs)
        
        try:
            # Execute the code
            exec(self.code, exec_globals, exec_locals)
            
            # Return any result (assuming the code stores it in a variable called 'result')
            return exec_locals.get('result', None)
        except Exception as e:
            return {"error": str(e), "error_type": type(e).__name__}

