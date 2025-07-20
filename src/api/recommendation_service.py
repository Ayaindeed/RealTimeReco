#!/usr/bin/env python3
"""
FastAPI Recommendation Service
Serves real-time personalized content recommendations
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'recommendation_engine'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'user_storage'))

from collaborative_filtering import CollaborativeFilteringEngine

app = FastAPI(title="Content Recommendation API", version="1.0.0")

# Global recommendation engine
recommendation_engine = None

class RecommendationRequest(BaseModel):
    user_id: str
    method: str = "hybrid"  # user_based, item_based, or hybrid
    n_recommendations: int = 10

class RecommendationResponse(BaseModel):
    user_id: str
    recommendations: List[dict]
    method: str
    total_found: int

class UserStatsResponse(BaseModel):
    total_users: int
    total_items: int
    sparsity_percent: float
    matrix_shape: tuple

@app.on_event("startup")
async def startup_event():
    """Initialize recommendation engine on startup"""
    global recommendation_engine
    print("Starting recommendation service...")
    recommendation_engine = CollaborativeFilteringEngine()
    
    # Build the user-item matrix
    matrix = recommendation_engine.build_user_item_matrix()
    if not matrix.empty:
        recommendation_engine.calculate_user_similarity()
        recommendation_engine.calculate_item_similarity()
        print("Recommendation engine ready!")
    else:
        print("Warning: No interaction data found!")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown"""
    global recommendation_engine
    if recommendation_engine:
        recommendation_engine.close()

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Content Recommendation API", "status": "active"}

@app.get("/stats", response_model=UserStatsResponse)
async def get_stats():
    """Get system statistics"""
    if not recommendation_engine or recommendation_engine.user_item_matrix is None:
        raise HTTPException(status_code=503, detail="Recommendation engine not ready")
    
    matrix = recommendation_engine.user_item_matrix
    total_elements = len(recommendation_engine.users) * len(recommendation_engine.items)
    non_zero_elements = (matrix != 0).sum().sum()
    sparsity = ((total_elements - non_zero_elements) / total_elements) * 100
    
    return UserStatsResponse(
        total_users=len(recommendation_engine.users),
        total_items=len(recommendation_engine.items),
        sparsity_percent=round(sparsity, 2),
        matrix_shape=(len(recommendation_engine.users), len(recommendation_engine.items))
    )

@app.get("/users")
async def get_users(limit: int = 10):
    """Get list of users in the system"""
    if not recommendation_engine:
        raise HTTPException(status_code=503, detail="Recommendation engine not ready")
    
    return {
        "users": recommendation_engine.users[:limit],
        "total_users": len(recommendation_engine.users)
    }

@app.post("/recommend", response_model=RecommendationResponse)
async def get_recommendations(request: RecommendationRequest):
    """Get personalized recommendations for a user"""
    if not recommendation_engine:
        raise HTTPException(status_code=503, detail="Recommendation engine not ready")
    
    if request.user_id not in recommendation_engine.users:
        raise HTTPException(status_code=404, detail=f"User {request.user_id} not found")
    
    # Get recommendations
    recs = recommendation_engine.get_user_recommendations(
        request.user_id, 
        method=request.method, 
        n_recommendations=request.n_recommendations
    )
    
    # Format response
    formatted_recs = []
    for content_id, score in recs:
        formatted_recs.append({
            "content_id": content_id,
            "predicted_rating": round(score, 3),
            "recommendation_reason": f"Based on {request.method} collaborative filtering"
        })
    
    return RecommendationResponse(
        user_id=request.user_id,
        recommendations=formatted_recs,
        method=request.method,
        total_found=len(formatted_recs)
    )

@app.get("/user/{user_id}/profile")
async def get_user_profile(user_id: str):
    """Get user interaction profile"""
    if not recommendation_engine:
        raise HTTPException(status_code=503, detail="Recommendation engine not ready")
    
    if user_id not in recommendation_engine.users:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    
    # Get user interactions
    user_interactions = recommendation_engine.user_item_matrix.loc[user_id]
    user_items = user_interactions[user_interactions > 0]
    
    interactions = []
    for content_id, rating in user_items.items():
        interactions.append({
            "content_id": content_id,
            "implicit_rating": round(rating, 2)
        })
    
    return {
        "user_id": user_id,
        "total_interactions": len(interactions),
        "interactions": interactions[:20]  # Limit to 20 for API response
    }

@app.get("/content/{content_id}/stats")
async def get_content_stats(content_id: str):
    """Get content interaction statistics"""
    if not recommendation_engine:
        raise HTTPException(status_code=503, detail="Recommendation engine not ready")
    
    if content_id not in recommendation_engine.items:
        raise HTTPException(status_code=404, detail=f"Content {content_id} not found")
    
    # Get content interactions
    content_interactions = recommendation_engine.user_item_matrix[content_id]
    interacted_users = content_interactions[content_interactions > 0]
    
    return {
        "content_id": content_id,
        "total_interactions": len(interacted_users),
        "average_rating": round(interacted_users.mean(), 2) if len(interacted_users) > 0 else 0,
        "interacted_users": list(interacted_users.index)[:10]  # Sample of users
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
