import pandas as pd

comments_file = "comments.csv"
comments_df = pd.read_csv(comments_file)

print(f"Old comment count: {len(comments_df)}")

new_comments_file = "comments_20241123_165603.csv"
new_comments_df = pd.read_csv(new_comments_file)

merged_df = pd.concat([comments_df, new_comments_df])

merged_df = merged_df.drop_duplicates(subset='id', keep='last')

print(f"Merged comment count: {len(merged_df)}")

merged_df.to_csv(comments_file, index=False)

print(f"Merged data saved to {comments_file}")