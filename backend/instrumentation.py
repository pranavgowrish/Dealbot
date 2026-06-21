# instrumentation.py
from dotenv import load_dotenv
load_dotenv()

import os
from arize.otel import register
from openinference.instrumentation.langchain import LangChainInstrumentor

tracer_provider = register(
    space_id=os.environ["ARIZE_SPACE_ID"],
    api_key=os.environ["ARIZE_API_KEY"],
    project_name=os.environ["ARIZE_PROJECT_NAME"],
)

LangChainInstrumentor().instrument(tracer_provider=tracer_provider)
print("Arize AX tracing initialized for LangGraph.")