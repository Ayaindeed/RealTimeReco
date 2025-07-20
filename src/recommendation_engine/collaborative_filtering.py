#!/usr/bin/env python3
"""
Collaborative Filtering Recommendation Engine
Implements user-based and item-based collaborative filtering for content recommendations
"""
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'user_storage'))

from hbase_manager import HBaseUserStorage

class CollaborativeFilteringEngine:
    """
    Collaborative filtering recommendation engine
    Supports both user-based and item-based filtering
    """
    
    def __init__(self):
        self.storage = HBaseUserStorage()
        self.user_item_matrix = None
        self.user_similarity_matrix = None
        self.item_similarity_matrix = None
        self.users = []
        self.items = []
        
        print("Collaborative Filtering Engine initialized")
    
    def build_user_item_matrix(self) -> pd.DataFrame:
        """Build user-item interaction matrix from HBase data"""
        print("Building user-item interaction matrix...")
        
        # Get all user interactions
        interactions_data = []
        
        # Scan user_interactions table
        for key, data in self.storage.user_interactions_table.scan():
            # Debug: print first few entries
            if len(interactions_data) < 5:
                print(f"Debug - Key: {key}, Data: {data}")
            
            user_id = '_'.join(key.decode().split('_')[:2])  # user_00001 instead of just user
            content_id = data.get(b'events:content_id', b'').decode()
            event_type = data.get(b'events:event_type', b'').decode()
            
            # Create implicit rating based on interaction type
            rating = self._calculate_implicit_rating(data, event_type)
            
            if rating > 0:
                interactions_data.append({
                    'user_id': user_id,
                    'content_id': content_id,
                    'rating': rating
                })
        
        # Convert to DataFrame
        df = pd.DataFrame(interactions_data)
        
        if df.empty:
            print("No interaction data found!")
            return pd.DataFrame()
        
        # Debug: Show unique users found
        unique_users = df['user_id'].unique()
        print(f"Debug - Found {len(unique_users)} unique users: {unique_users[:10]}")
        print(f"Debug - Total interactions: {len(df)}")
        
        # Aggregate multiple interactions per user-item pair
        df = df.groupby(['user_id', 'content_id'])['rating'].mean().reset_index()
        
        # Create user-item matrix
        self.user_item_matrix = df.pivot(index='user_id', columns='content_id', values='rating').fillna(0)
        self.users = list(self.user_item_matrix.index)
        self.items = list(self.user_item_matrix.columns)
        
        print(f"Matrix built: {len(self.users)} users x {len(self.items)} items")
        print(f"Sparsity: {(self.user_item_matrix == 0).sum().sum() / (len(self.users) * len(self.items)) * 100:.1f}%")
        
        return self.user_item_matrix
    
    def _calculate_implicit_rating(self, data: Dict, event_type: str) -> float:
        """Calculate implicit rating from interaction data"""
        rating = 0.0
        
        if event_type == 'view':
            # Base rating for view
            rating = 1.0
            
            # Bonus for watch duration
            if b'events:watch_duration' in data:
                watch_duration = int(data[b'events:watch_duration'].decode())
                if watch_duration > 300:  # 5+ minutes
                    rating = 2.0
                if watch_duration > 1800:  # 30+ minutes
                    rating = 3.0
                if watch_duration > 3600:  # 1+ hour
                    rating = 4.0
        
        elif event_type == 'like':
            rating = 3.0
        
        elif event_type == 'rating':
            if b'ratings:score' in data:
                rating = float(data[b'ratings:score'].decode())
        
        elif event_type == 'click':
            rating = 0.5
        
        elif event_type == 'search':
            rating = 0.3
        
        return rating
    
    def calculate_user_similarity(self) -> np.ndarray:
        """Calculate user-user similarity matrix using cosine similarity"""
        if self.user_item_matrix is None:
            self.build_user_item_matrix()
        
        print("Calculating user similarity matrix...")
        
        # Cosine similarity
        matrix = self.user_item_matrix.values
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1  # Avoid division by zero
        normalized_matrix = matrix / norms
        
        self.user_similarity_matrix = np.dot(normalized_matrix, normalized_matrix.T)
        
        print(f"User similarity matrix calculated: {self.user_similarity_matrix.shape}")
        return self.user_similarity_matrix
    
    def calculate_item_similarity(self) -> np.ndarray:
        """Calculate item-item similarity matrix using cosine similarity"""
        if self.user_item_matrix is None:
            self.build_user_item_matrix()
        
        print("Calculating item similarity matrix...")
        
        # Transpose for item-item similarity
        matrix = self.user_item_matrix.values.T
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1
        normalized_matrix = matrix / norms
        
        self.item_similarity_matrix = np.dot(normalized_matrix, normalized_matrix.T)
        
        print(f"Item similarity matrix calculated: {self.item_similarity_matrix.shape}")
        return self.item_similarity_matrix
    
    def recommend_user_based(self, user_id: str, n_recommendations: int = 10) -> List[Tuple[str, float]]:
        """Generate recommendations using user-based collaborative filtering"""
        if user_id not in self.users:
            print(f"User {user_id} not found in training data")
            return []
        
        if self.user_similarity_matrix is None:
            self.calculate_user_similarity()
        
        user_idx = self.users.index(user_id)
        user_similarities = self.user_similarity_matrix[user_idx]
        user_ratings = self.user_item_matrix.iloc[user_idx].values
        
        # Find items user hasn't interacted with
        unrated_items = np.where(user_ratings == 0)[0]
        
        recommendations = []
        
        for item_idx in unrated_items:
            # Calculate predicted rating
            item_ratings = self.user_item_matrix.iloc[:, item_idx].values
            rated_users = np.where(item_ratings > 0)[0]
            
            if len(rated_users) == 0:
                continue
            
            # Weighted average of similar users' ratings
            numerator = np.sum(user_similarities[rated_users] * item_ratings[rated_users])
            denominator = np.sum(np.abs(user_similarities[rated_users]))
            
            if denominator > 0:
                predicted_rating = numerator / denominator
                content_id = self.items[item_idx]
                recommendations.append((content_id, predicted_rating))
        
        # Sort by predicted rating
        recommendations.sort(key=lambda x: x[1], reverse=True)
        return recommendations[:n_recommendations]
    
    def recommend_item_based(self, user_id: str, n_recommendations: int = 10) -> List[Tuple[str, float]]:
        """Generate recommendations using item-based collaborative filtering"""
        if user_id not in self.users:
            print(f"User {user_id} not found in training data")
            return []
        
        if self.item_similarity_matrix is None:
            self.calculate_item_similarity()
        
        user_idx = self.users.index(user_id)
        user_ratings = self.user_item_matrix.iloc[user_idx].values
        
        # Find items user has interacted with
        rated_items = np.where(user_ratings > 0)[0]
        unrated_items = np.where(user_ratings == 0)[0]
        
        recommendations = []
        
        for item_idx in unrated_items:
            # Calculate predicted rating based on similar items
            item_similarities = self.item_similarity_matrix[item_idx]
            
            numerator = 0
            denominator = 0
            
            for rated_item_idx in rated_items:
                similarity = item_similarities[rated_item_idx]
                rating = user_ratings[rated_item_idx]
                
                numerator += similarity * rating
                denominator += abs(similarity)
            
            if denominator > 0:
                predicted_rating = numerator / denominator
                content_id = self.items[item_idx]
                recommendations.append((content_id, predicted_rating))
        
        # Sort by predicted rating
        recommendations.sort(key=lambda x: x[1], reverse=True)
        return recommendations[:n_recommendations]
    
    def get_user_recommendations(self, user_id: str, method: str = 'hybrid', n_recommendations: int = 10) -> List[Tuple[str, float]]:
        """Get recommendations for a user using specified method"""
        if method == 'user_based':
            return self.recommend_user_based(user_id, n_recommendations)
        elif method == 'item_based':
            return self.recommend_item_based(user_id, n_recommendations)
        elif method == 'hybrid':
            # Combine both methods
            user_recs = self.recommend_user_based(user_id, n_recommendations * 2)
            item_recs = self.recommend_item_based(user_id, n_recommendations * 2)
            
            # Combine and average scores
            combined = defaultdict(list)
            for content_id, score in user_recs:
                combined[content_id].append(score)
            for content_id, score in item_recs:
                combined[content_id].append(score)
            
            # Average scores and sort
            final_recs = []
            for content_id, scores in combined.items():
                avg_score = np.mean(scores)
                final_recs.append((content_id, avg_score))
            
            final_recs.sort(key=lambda x: x[1], reverse=True)
            return final_recs[:n_recommendations]
        else:
            raise ValueError("Method must be 'user_based', 'item_based', or 'hybrid'")
    
    def close(self):
        """Close storage connection"""
        self.storage.close()

if __name__ == "__main__":
    # Test the recommendation engine
    engine = CollaborativeFilteringEngine()
    
    # Build the user-item matrix
    matrix = engine.build_user_item_matrix()
    
    if not matrix.empty:
        # Calculate similarities
        engine.calculate_user_similarity()
        engine.calculate_item_similarity()
        
        # Test recommendations for a random user
        test_user = engine.users[0] if engine.users else None
        if test_user:
            print(f"\nTesting recommendations for user: {test_user}")
            
            # Debug: Show user's interactions
            user_interactions = engine.user_item_matrix.loc[test_user]
            user_items = user_interactions[user_interactions > 0]
            print(f"User {test_user} has {len(user_items)} interactions: {list(user_items.index)[:5]}")
            
            # Debug: Find users with overlapping items
            overlapping_users = []
            for item in user_items.index[:3]:  # Check first 3 items
                other_users = engine.user_item_matrix[item][engine.user_item_matrix[item] > 0].index
                overlapping_users.extend([u for u in other_users if u != test_user])
            print(f"Found {len(set(overlapping_users))} users with overlapping items")
            
            user_based_recs = engine.get_user_recommendations(test_user, method='user_based', n_recommendations=5)
            print(f"User-based recommendations: {user_based_recs}")
            
            item_based_recs = engine.get_user_recommendations(test_user, method='item_based', n_recommendations=5)
            print(f"Item-based recommendations: {item_based_recs}")
            
            hybrid_recs = engine.get_user_recommendations(test_user, method='hybrid', n_recommendations=5)
            print(f"Hybrid recommendations: {hybrid_recs}")
    
    engine.close()
