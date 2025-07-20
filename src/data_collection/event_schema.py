#!/usr/bin/env python3
"""
Event Schema for Real-Time Content Recommendation System
Defines the structure of user interaction events for streaming platform
"""
from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime
import json

@dataclass
class UserInteractionEvent:
    """
    Core event structure for user interactions with content
    Used for collaborative filtering and recommendation generation
    """
    user_id: str
    content_id: str
    event_type: str  # 'view', 'click', 'like', 'search', 'share', 'rating'
    timestamp: str
    session_id: str
    
    # Content metadata
    content_category: Optional[str] = None
    content_duration: Optional[int] = None  # seconds
    content_genre: Optional[str] = None
    
    # Interaction details
    watch_duration: Optional[int] = None  # seconds actually watched
    rating_score: Optional[float] = None  # 1-5 stars
    search_query: Optional[str] = None
    
    # Context
    device_type: Optional[str] = None
    user_location: Optional[str] = None
    
    # Recommendation context
    recommended_by: Optional[str] = None  # algorithm that recommended this content
    recommendation_score: Optional[float] = None
    
    def to_kafka_message(self) -> Dict[str, Any]:
        """Convert to Kafka message format"""
        return {
            'user_id': self.user_id,
            'content_id': self.content_id,
            'event_type': self.event_type,
            'timestamp': self.timestamp,
            'session_id': self.session_id,
            'content_category': self.content_category,
            'content_duration': self.content_duration,
            'content_genre': self.content_genre,
            'watch_duration': self.watch_duration,
            'rating_score': self.rating_score,
            'search_query': self.search_query,
            'device_type': self.device_type,
            'user_location': self.user_location,
            'recommended_by': self.recommended_by,
            'recommendation_score': self.recommendation_score
        }
    
    @classmethod
    def from_kafka_message(cls, message: Dict[str, Any]) -> 'UserInteractionEvent':
        """Create from Kafka message"""
        return cls(
            user_id=message['user_id'],
            content_id=message['content_id'],
            event_type=message['event_type'],
            timestamp=message['timestamp'],
            session_id=message['session_id'],
            content_category=message.get('content_category'),
            content_duration=message.get('content_duration'),
            content_genre=message.get('content_genre'),
            watch_duration=message.get('watch_duration'),
            rating_score=message.get('rating_score'),
            search_query=message.get('search_query'),
            device_type=message.get('device_type'),
            user_location=message.get('user_location'),
            recommended_by=message.get('recommended_by'),
            recommendation_score=message.get('recommendation_score')
        )

# Event type constants
class EventTypes:
    VIEW = 'view'
    CLICK = 'click'
    LIKE = 'like'
    SEARCH = 'search'
    SHARE = 'share'
    RATING = 'rating'
    SKIP = 'skip'
    FAVORITE = 'favorite'
    PURCHASE = 'purchase'
    DOWNLOAD = 'download'
