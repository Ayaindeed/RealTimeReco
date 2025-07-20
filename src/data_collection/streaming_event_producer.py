#!/usr/bin/env python3
"""
Streaming Platform Event Producer
Generates realistic user interaction events for content recommendation system
"""
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Dict
from kafka import KafkaProducer
from event_schema import UserInteractionEvent, EventTypes

class StreamingEventProducer:
    """
    Produces realistic streaming platform events for recommendation system testing
    Simulates user interactions: views, clicks, likes, searches, ratings
    """
    
    def __init__(self, kafka_servers: List[str] = ['kafka:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Simulated content catalog
        self.content_catalog = self._generate_content_catalog()
        
        # Simulated user profiles
        self.user_profiles = self._generate_user_profiles()
        
        # Session tracking
        self.active_sessions = {}
        
        print(f"StreamingEventProducer initialized with {len(self.content_catalog)} content items and {len(self.user_profiles)} users")
    
    def _generate_content_catalog(self) -> List[Dict]:
        """Generate realistic content catalog"""
        genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Romance', 'Sci-Fi', 'Documentary', 'Animation']
        categories = ['Movie', 'TV Series', 'Short Film', 'Documentary', 'Music Video']
        
        catalog = []
        for i in range(1000):  # 1000 pieces of content
            content = {
                'content_id': f'content_{i:04d}',
                'title': f'Content Title {i}',
                'genre': random.choice(genres),
                'category': random.choice(categories),
                'duration': random.randint(300, 7200),  # 5 minutes to 2 hours
                'release_year': random.randint(2010, 2024),
                'popularity_score': random.uniform(0.1, 1.0)
            }
            catalog.append(content)
        return catalog
    
    def _generate_user_profiles(self) -> List[Dict]:
        """Generate realistic user profiles with preferences"""
        profiles = []
        genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Romance', 'Sci-Fi', 'Documentary', 'Animation']
        devices = ['mobile', 'tablet', 'desktop', 'smart_tv', 'gaming_console']
        locations = ['US', 'UK', 'CA', 'DE', 'FR', 'JP', 'AU', 'BR', 'IN', 'KR']
        
        for i in range(10000):  # 10,000 users
            user = {
                'user_id': f'user_{i:05d}',
                'age_group': random.choice(['18-25', '26-35', '36-45', '46-55', '55+']),
                'preferred_genres': random.sample(genres, random.randint(2, 4)),
                'preferred_device': random.choice(devices),
                'location': random.choice(locations),
                'subscription_tier': random.choice(['free', 'premium', 'family']),
                'activity_level': random.choice(['low', 'medium', 'high'])
            }
            profiles.append(user)
        return profiles
    
    def _get_user_session(self, user_id: str) -> str:
        """Get or create user session"""
        if user_id not in self.active_sessions:
            self.active_sessions[user_id] = {
                'session_id': str(uuid.uuid4()),
                'start_time': datetime.now(),
                'events_count': 0
            }
        
        session = self.active_sessions[user_id]
        session['events_count'] += 1
        
        # End session after 50 events or 2 hours
        if (session['events_count'] > 50 or 
            datetime.now() - session['start_time'] > timedelta(hours=2)):
            self.active_sessions[user_id] = {
                'session_id': str(uuid.uuid4()),
                'start_time': datetime.now(),
                'events_count': 1
            }
        
        return self.active_sessions[user_id]['session_id']
    
    def _generate_realistic_event(self) -> UserInteractionEvent:
        """Generate a realistic user interaction event"""
        user = random.choice(self.user_profiles)
        content = random.choice(self.content_catalog)
        
        # Bias content selection based on user preferences
        if random.random() < 0.7:  # 70% chance to pick preferred genre
            preferred_content = [c for c in self.content_catalog if c['genre'] in user['preferred_genres']]
            if preferred_content:
                content = random.choice(preferred_content)
        
        event_type = random.choices(
            [EventTypes.VIEW, EventTypes.CLICK, EventTypes.LIKE, EventTypes.SEARCH, EventTypes.RATING],
            weights=[40, 20, 15, 15, 10]  # View events are most common
        )[0]
        
        session_id = self._get_user_session(user['user_id'])
        timestamp = datetime.now().isoformat()
        
        # Generate event-specific data
        watch_duration = None
        rating_score = None
        search_query = None
        
        if event_type == EventTypes.VIEW:
            # Realistic watch duration based on content length
            max_watch = content['duration']
            if random.random() < 0.8:  # 80% watch some portion
                watch_duration = random.randint(30, min(max_watch, 3600))
            else:  # 20% watch very little (skip)
                watch_duration = random.randint(5, 60)
        
        elif event_type == EventTypes.RATING:
            # Bias ratings based on genre preference
            if content['genre'] in user['preferred_genres']:
                rating_score = random.uniform(3.5, 5.0)
            else:
                rating_score = random.uniform(1.0, 4.0)
        
        elif event_type == EventTypes.SEARCH:
            search_queries = [
                'action movies', 'romantic comedy', 'horror films', 'documentary series',
                'new releases', 'trending now', 'sci-fi thriller', 'animated movies',
                content['genre'].lower(), f"{content['genre'].lower()} {content['category'].lower()}"
            ]
            search_query = random.choice(search_queries)
        
        return UserInteractionEvent(
            user_id=user['user_id'],
            content_id=content['content_id'],
            event_type=event_type,
            timestamp=timestamp,
            session_id=session_id,
            content_category=content['category'],
            content_duration=content['duration'],
            content_genre=content['genre'],
            watch_duration=watch_duration,
            rating_score=rating_score,
            search_query=search_query,
            device_type=user['preferred_device'],
            user_location=user['location']
        )
    
    def produce_events(self, num_events: int = 100, delay_seconds: float = 0.1):
        """Produce a stream of realistic events"""
        print(f"Starting to produce {num_events} events with {delay_seconds}s delay...")
        
        for i in range(num_events):
            try:
                event = self._generate_realistic_event()
                message = event.to_kafka_message()
                
                # Send to Kafka topic
                self.producer.send('user-interactions', value=message)
                
                # Print interesting events
                if event.event_type == EventTypes.VIEW and event.watch_duration and event.watch_duration > 1800:
                    print(f"Long watch: {event.user_id} watched {event.content_id} for {event.watch_duration//60} minutes")
                elif event.event_type == EventTypes.RATING and event.rating_score and event.rating_score >= 4.5:
                    print(f"High rating: {event.user_id} rated {event.content_id} {event.rating_score:.1f} stars")
                elif event.event_type == EventTypes.SEARCH:
                    print(f"Search: {event.user_id} searched for '{event.search_query}'")
                
                if (i + 1) % 100 == 0:
                    print(f"Produced {i + 1} events...")
                
                time.sleep(delay_seconds)
                
            except Exception as e:
                print(f"Error producing event {i}: {e}")
        
        # Flush remaining messages
        self.producer.flush()
        print(f"Successfully produced {num_events} events")
    
    def produce_continuous_stream(self, events_per_second: float = 10):
        """Produce continuous stream of events"""
        print(f"Starting continuous stream at {events_per_second} events/second...")
        delay = 1.0 / events_per_second
        
        try:
            while True:
                event = self._generate_realistic_event()
                message = event.to_kafka_message()
                self.producer.send('user-interactions', value=message)
                time.sleep(delay)
        except KeyboardInterrupt:
            print("\nStopping continuous stream...")
        finally:
            self.producer.flush()
            self.producer.close()

if __name__ == "__main__":
    producer = StreamingEventProducer()
    
    # Generate 1000 events for testing
    producer.produce_events(num_events=1000, delay_seconds=0.01)
