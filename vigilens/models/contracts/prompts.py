from pydantic import BaseModel, Field
from typing import List


class VideoAnalysisResult(BaseModel):
    video_path: str = Field(description="The path / chunk id of the video")
    analysis: str = Field(description="The analysis of the video")
    title: str = Field(
        description="The title of the analysis / brief description of the analysis in 5-10 words"
    )
    key_identifiers: List[str] = Field(
        description="The key identifiers of the video - names / identification of objects , people etc"
    )
    key_frame_numbers: List[int] = Field(
        description="The frame numbers of the key identifiers"
    )
    is_action_detected: bool = Field(
        description="Whether the action in the query is detected in the video"
    )
    is_continuation: bool = Field(
        description="Whether the video is a continuation of the previous detection"
    )


class VideoAnalysisResultList(BaseModel):
    results: List[VideoAnalysisResult] = Field(
        description="The analysis results of the videos"
    )
    is_action_detected: bool = Field(
        description="Whether the action in the query is detected in the videos"
    )


LLM_VIDEO_ANALYSIS_PROMPT = """
You are an expert video analyst, who is analyzing a live video to detect a specific action / event. You will be given a candidate chunks of videos from a live stream. 
Your task is to analyze the video and determine whether the video contains the query.
If the video contains the query, you should return the video and frames that contains the query.
If the video does not contain the query, you should return None.

History Context:
- You are given a history of previous video analysis results for the same stream. Use the history to understand the context of the video for accurate analysis.
- If the current videos contain a continuation or a residue of the previous detection, you should return is_continuation as True.
- If the current videos do not contain a continuation or a residue of the previous detection and it is a completely independent action / event, you should return is_continuation as False.

Think carefully before responding
"""

RERANKER_INSTRUCTION = """Retrieve the most relevant video chunks with user's query"""
