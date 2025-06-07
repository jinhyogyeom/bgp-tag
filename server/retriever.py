#!/usr/bin/env python3
import os
from langchain_community.vectorstores import Milvus
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.chat_models import ChatOllama
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI


def get_retriever(embedding_model: str, k: int, target_date: str):
    vectorstore = Milvus(
        embedding_function=HuggingFaceEmbeddings(model_name=embedding_model),
        collection_name=f"bgp_reports_{target_date}",
        connection_args={
            "host": os.getenv("MILVUS_HOST", "milvus"),
            "port": os.getenv("MILVUS_PORT", "19530"),
        },
    )
    return vectorstore.as_retriever(search_kwargs={"k": k})


def get_chain(retriever, llm_model: str, start_datetime: str, end_datetime: str):
    prompt = ChatPromptTemplate.from_template(
        """
        You are a network analysis assistant specialized in BGP anomaly detection.
        Your task is to help the user understand BGP-related events using only the information provided in the documents.

        In addition to analyzing the risk, you are an expert in BGP anomalies, so please provide a **detailed analysis** of the report content.

        Always respond **in Korean**, and do not invent or assume any information not explicitly included in the documents.
        Use only the facts found in the context, and explain clearly and professionally.

        Here is the information you have:
        "A

        User Query:
        "{query}"

        Reference Documents:
        {context}

        Start Date and Time: {start_datetime}
        End Date and Time: {end_datetime}

        Answer the question in Korean.
        If multiple anomalies are involved (e.g., hijack, flap, loop, MOAS), summarize each clearly, including:
        - anomaly type (in Korean),
        - affected prefixes,
        - time range,
        - update count or risk score if available.

        Use bullet points for readability. If no relevant information is found, respond politely saying that no relevant data was available in the provided context.
        """
    )

    if llm_model.startswith("gpt-"):
        llm = ChatOpenAI(
            model=llm_model,
            temperature=0.3,
            openai_api_key=os.getenv("OPENAI_API_KEY"),
        )
    else:
        llm = ChatOllama(model=llm_model, base_url=os.getenv("OLLAMA_BASE_URL"))

    output_parser = StrOutputParser()

    chain = (
        {
            "context": retriever,
            "query": RunnablePassthrough(),
            "start_datetime": lambda x: start_datetime,
            "end_datetime": lambda x: end_datetime,
        }
        | prompt
        | llm
        | output_parser
    )
    return chain


def rag_chain(
    query: str,
    embedding_model: str,
    llm_model: str,
    k: int,
    target_date: str,
    start_datetime: str,
    end_datetime: str,
):
    retriever = get_retriever(embedding_model, k, target_date)
    chain = get_chain(
        retriever=retriever,
        llm_model=llm_model,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )
    return chain.invoke(query)
