#!/usr/bin/env python3
"""
Stream Processor for User Interactions
Consumes Kafka events and stores them in HBase for recommendation system
"""
import json
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_collection'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'user_storage'))

from kafka import KafkaConsumer
from event_schema import UserInteractionEvent
from hbase_manager import HBaseUserStorage

class UserInteractionProcessor:
    """
    Processes user interaction events from Kafka and stores them in HBase
    """
    
    def __init__(self):
        # Kafka consumer
        self.consumer = KafkaConsumer(
            'user-interactions',
            bootstrap_servers=['kafka:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='user-interaction-processor',
            auto_offset_reset='earliest'
        )
        
        # HBase storage
        self.storage = HBaseUserStorage()
        
        # Statistics
        self.processed_events = 0
        self.stored_profiles = set()
        
        print("User Interaction Processor initialized")
    
    def process_events(self):
        """Main processing loop"""
        print("Starting to process user interaction events...")
        
        try:
            for message in self.consumer:
                event_data = message.value
                
                # Parse event
                event = UserInteractionEvent.from_kafka_message(event_data)
                
                # Store user profile if not seen before
                if event.user_id not in self.stored_profiles:
                    self._store_user_profile(event)
                    self.stored_profiles.add(event.user_id)
                
                # Store interaction
                self.storage.store_user_interaction(event.user_id, event_data)
                
                self.processed_events += 1
                
                if self.processed_events % 100 == 0:
                    print(f"Processed {self.processed_events} events, stored {len(self.stored_profiles)} user profiles")
                
                # Print interesting events
                if event.event_type == 'view' and event.watch_duration and event.watch_duration > 1800:
                    print(f"Long watch stored: {event.user_id} -> {event.content_id} ({event.watch_duration//60}min)")
                elif event.event_type == 'rating' and event.rating_score and event.rating_score >= 4.5:
                    print(f"High rating stored: {event.user_id} -> {event.content_id} ({event.rating_score:.1f}★)")
        
        except KeyboardInterrupt:
            print(f"\nStopping processor. Processed {self.processed_events} events total.")
        finally:
            self.consumer.close()
            self.storage.close()
    
    def _store_user_profile(self, event: UserInteractionEvent):
        """Extract and store user profile from event"""
        profile_data = {
            'location': event.user_location or 'unknown',
            'preferred_device': event.device_type or 'unknown',
            'subscription_tier': 'unknown',  # Would come from user service
            'age_group': 'unknown',  # Would come from user service
            'preferred_genres': [],  # Will be learned from interactions
            'activity_level': 'unknown'  # Will be calculated
        }
        
        self.storage.store_user_profile(event.user_id, profile_data)

if __name__ == "__main__":
    processor = UserInteractionProcessor()
    processor.process_events()
