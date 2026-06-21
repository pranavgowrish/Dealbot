import operator
import uuid
import asyncio
from typing import Annotated, Literal, TypedDict
from langchain_core.messages import AnyMessage, AIMessage, SystemMessage, HumanMessage
from langgraph.graph import StateGraph, START, END
from langchain.chat_models import init_chat_model
from langchain.tools import tool

model = init_chat_model("claude-3-5-sonnet-20241022", temperature=0.0)

class WorkerState(TypedDict):
    worker_id: str          
    target_seller: str      
    current_min_price: float 
    messages: Annotated[list[AnyMessage], operator.add]
    seller_budged: bool

def create_and_send(state: WorkerState):
    """Use memory from redis and global state to create & send a message"""
    worker_id = state["worker_id"]
    target_seller = state["target_seller"]
    current_min_price = state["current_min_price"]
    
    # 1. Fetch this specific worker's long-term chat history from Redis
    # existing_chat = get_message_history_from_redis_agent_memory(worker_id)
    existing_chat = [] # Placeholder fallback
    
    # 2. Fetch the global competitor floor price from Redis 
    global_lowest_price = 80.0  # Placeholder fallback value
    
    # 3. Construct the system instructions
    system_instruction = SystemMessage(content=f"""
    You are a professional negotiation agent acting on behalf of a buyer.
    You are currently messaging the seller: {target_seller}.
    
    Your current target price floor for this item is ${current_min_price}. Do not offer below this.
    
    MARKET INTELLIGENCE LEVERAGE: 
    Another seller has already agreed to sell us this exact product for ${global_lowest_price}. 
    If the current seller's price is higher than ${global_lowest_price}, use this information 
    strategically as leverage to get them to match or beat it. Do not mention the other seller's name.
    """)
    
    full_prompt = [system_instruction] + existing_chat
    
    # 4. Invoke the globally defined model
    ai_response = model.invoke(full_prompt)
    message_content = ai_response.content
    
    # 5. Perform the side-effect action
    send_message_via_browserbase(target_seller, message_content)
    # UPDATE REDIS MEMORY
    
    return {"messages": [AIMessage(content=message_content)]}


def evaluate_response(state: WorkerState):
    """Wait for reply, read msg -> Will seller budge?"""
    reply = wait_for_reply_from_seller(state["target_seller"])
    # Place your actual checking logic here

    existing_chat = [] # FIX GET FROM REDIS

    system_instruction = SystemMessage(content=f"""
    You are a professional negotiation agent acting on behalf of a buyer.
    You are currently messaging the seller: {state['target_seller']}.
    You have received a reply from the seller. 
    Here is the seller's reply: "{reply}"
    Here is the rest of the chat: "{existing_chat}"
    Based on this reply and the rest of the chat, will the seller budge on their price? Answer ONLY WITH A SIMPLE "Yes" OR "No".
    """)

    ai_resonse = model.invoke([system_instruction])
    answer = ai_resonse.content.strip().lower()

    if (answer == "yes"):
        budged = True
    else:
        budged = False
        
    # FIX 1: Return the reply as a HumanMessage so LangGraph remembers it
    return {
        "seller_budged": budged,
        "messages": [HumanMessage(content=reply)]
    }


def route_evaluation(state: WorkerState) -> Literal["update_memory", "wrap_up"]:
    """Conditional router based on 'Will seller budge?'"""
    if state.get("seller_budged"):
        return "update_memory"
    return "wrap_up"


def update_memory(state: WorkerState):
    """Update redis memory with chat history + new min price"""
    # FIX 2: Calculate the new min price (e.g., dropping it by 10%)
    new_price = state["current_min_price"] * 0.9 
    
    # Save to Redis here...
    
    return {"current_min_price": new_price}


def wrap_up(state: WorkerState):
    """Ty for your time -> Kill worker"""
    final_message = "Ty for your time"
    
    # FIX 3: Actually send the final message out via your tool before closing
    send_message_via_browserbase(state["target_seller"], final_message)
    
    return {"messages": [AIMessage(content=final_message)]}




builder = StateGraph(WorkerState)

builder.add_node("create_and_send", create_and_send)
builder.add_node("evaluate", evaluate_response)
builder.add_node("update_memory", update_memory)
builder.add_node("wrap_up", wrap_up)

builder.add_edge(START, "create_and_send")
builder.add_edge("create_and_send", "evaluate")
builder.add_conditional_edges("evaluate", route_evaluation)
builder.add_edge("update_memory", "create_and_send")
builder.add_edge("wrap_up", END)

negotiation_agent = builder.compile()


async def launch_workers():
    inputs = [
        {"worker_id": "worker1", "target_seller": "Listing A", "current_min_price": 100.0},
        {"worker_id": "worker2", "target_seller": "Listing B", "current_min_price": 250.0},
        {"worker_id": "worker3", "target_seller": "Listing C", "current_min_price": 50.0},
        {"worker_id": "worker4", "target_seller": "Listing D", "current_min_price": 1200.0},
        {"worker_id": "worker5", "target_seller": "Listing E", "current_min_price": 75.0},
    ]
    
    # Run all 5 concurrently
    results = await negotiation_agent.abatch(inputs)
    return results

if __name__ == "__main__":
    final_results = asyncio.run(launch_workers())
    print(final_results)