"""
dataset_profile.py
Exploratory profiling of Dreaddit train and test datasets.
Analyzes schema, row counts, label distribution, timestamp ranges, and subreddit distribution.
"""

import os
import pandas as pd
import numpy as np

def profile_dataset(train_path: str, test_path: str):
    print("=" * 60)
    print("BDA-MH-K5: Dreaddit Dataset Profiling Report")
    print("=" * 60)
    
    if not os.path.exists(train_path):
        raise FileNotFoundError(f"Train dataset not found at {train_path}")
    if not os.path.exists(test_path):
        raise FileNotFoundError(f"Test dataset not found at {test_path}")
        
    df_train = pd.read_csv(train_path)
    df_test = pd.read_csv(test_path)
    
    print(f"\n[1] Record Counts:")
    print(f"  - Train records: {len(df_train):,}")
    print(f"  - Test records:  {len(df_test):,}")
    print(f"  - Total records: {len(df_train) + len(df_test):,}")
    
    # Required Phase 1 columns
    required_cols = [
        "post_id", "subreddit", "social_timestamp", "text",
        "label", "confidence", "social_karma", "social_num_comments", "syntax_ari"
    ]
    
    print(f"\n[2] Schema Verification for Required Columns:")
    for col in required_cols:
        in_train = col in df_train.columns
        in_test = col in df_test.columns
        print(f"  - {col:<22}: Train={in_train!s:<5} Test={in_test!s:<5}")
        
    # Combine required subset for profiling
    df_combined = pd.concat([df_train[required_cols], df_test[required_cols]], ignore_index=True)
    
    print(f"\n[3] Missing / Null Values:")
    null_counts = df_combined.isnull().sum()
    for col, count in null_counts.items():
        print(f"  - {col:<22}: {count} nulls ({count / len(df_combined):.2%})")
        
    print(f"\n[4] Target Label Semantics & Distribution:")
    # In Dreaddit: 1 = Stressed, 0 = Non-stressed / control
    label_counts = df_combined['label'].value_counts()
    for lbl, cnt in label_counts.items():
        status = "Stress (1)" if lbl == 1 else "Non-Stress (0)"
        print(f"  - {status}: {cnt:,} ({cnt / len(df_combined):.2%})")
        
    print(f"\n[5] Confidence Score Distribution:")
    conf_stats = df_combined['confidence'].describe()
    for stat, val in conf_stats.items():
        print(f"  - {stat:<10}: {val:.4f}")
        
    print(f"\n[6] Subreddit Distribution (Top 10):")
    sub_counts = df_combined['subreddit'].value_counts()
    for sub, cnt in sub_counts.items():
        sub_stress_rate = df_combined[df_combined['subreddit'] == sub]['label'].mean()
        print(f"  - r/{sub:<20}: {cnt:>5} posts | Stress Rate: {sub_stress_rate:.2%}")
        
    print(f"\n[7] Temporal Range (social_timestamp):")
    min_ts = df_combined['social_timestamp'].min()
    max_ts = df_combined['social_timestamp'].max()
    min_dt = pd.to_datetime(min_ts, unit='s')
    max_dt = pd.to_datetime(max_ts, unit='s')
    print(f"  - Min Timestamp: {min_ts} ({min_dt.strftime('%Y-%m-%d %H:%M:%S UTC')})")
    print(f"  - Max Timestamp: {max_ts} ({max_dt.strftime('%Y-%m-%d %H:%M:%S UTC')})")
    
    print(f"\n[8] Social Engagement & Readability Statistics:")
    print(f"  - Social Karma:       Min={df_combined['social_karma'].min()}, Median={df_combined['social_karma'].median()}, Max={df_combined['social_karma'].max()}")
    print(f"  - Social Num Comments: Min={df_combined['social_num_comments'].min()}, Median={df_combined['social_num_comments'].median()}, Max={df_combined['social_num_comments'].max()}")
    print(f"  - Syntax ARI:         Min={df_combined['syntax_ari'].min():.2f}, Mean={df_combined['syntax_ari'].mean():.2f}, Max={df_combined['syntax_ari'].max():.2f}")
    
    zero_comments = (df_combined['social_num_comments'] == 0).sum()
    print(f"  - Posts with 0 comments: {zero_comments} ({zero_comments / len(df_combined):.2%}) [requires explicit handling in karma ratio]")
    print("=" * 60)
    print("Profiling Complete.\n")

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    train_file = os.path.join(project_root, "data", "raw", "train.csv")
    test_file = os.path.join(project_root, "data", "raw", "test.csv")
    profile_dataset(train_file, test_file)
