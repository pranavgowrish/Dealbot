# example.py

# Importing instrumentation first ensures tracing is set up
# before `langgraph` is imported.
from instrumentation import tracer_provider

from langchain_core.tools import tool
from langchain_anthropic import ChatAnthropic
from langgraph.prebuilt import create_react_agent

@tool
def get_weather(city: str) -> str:
    """Get the current weather for a city. Returns a short string."""
    if city.lower() in ("sf", "san francisco"):
        return "It's 60 degrees and foggy in San Francisco."
    return f"It's 75 degrees and sunny in {city}."


# ChatAnthropic reads ANTHROPIC_API_KEY from the environment.
agent = create_react_agent(
    model=ChatAnthropic(model="claude-sonnet-4-6"),
    tools=[get_weather],
)

result = agent.invoke({
    "messages": [("user", "What's the weather in San Francisco?")],
})

print(result["messages"][-1].content)