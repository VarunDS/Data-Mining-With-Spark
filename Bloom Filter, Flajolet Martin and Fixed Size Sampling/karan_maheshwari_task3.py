import tweepy
import random


class MyStreamListener(tweepy.StreamListener):

	def __init__(self):
		super().__init__()
		self.list_of_tweets = []
		self.round = 0
		self.hash_tag_counts = {}

	def update_hash_tag_count(self, hashtags):

		for hash_tag in hashtags:
			try:
				self.hash_tag_counts[hash_tag] += 1
			except KeyError:
				self.hash_tag_counts[hash_tag] = 1

		print("The number of tweets with tags from the beginning: "+str(self.round))
		liz = sorted(sorted(list(self.hash_tag_counts.items())), key=lambda x: x[1], reverse=True)[:3]
		for tag in liz:
			print(tag[0], " : ", tag[1])

		print("\n")

	def remove_status_hash_tags(self, status):

		hash_tags = [i['text'] for i in status.entities.get('hashtags')]
		for hash_tag in hash_tags:
			self.hash_tag_counts[hash_tag] -= 1
			if self.hash_tag_counts[hash_tag] == 0:
				del(self.hash_tag_counts[hash_tag])

	def on_status(self, status):

		hash_tags = [i['text'] for i in status.entities.get('hashtags')]
		if hash_tags:
			if self.round <= 100:
				self.round += 1
				self.list_of_tweets.append(status)
				self.update_hash_tag_count(hash_tags)
			else:
				self.reservoir_sampling(status, hash_tags)

	def reservoir_sampling(self, status, hash_tags):

		prob = random.randint(0, self.round)
		if prob < 100:
			status_to_be_removed = self.list_of_tweets[prob]
			self.list_of_tweets[prob] = status
			self.round += 1
			self.remove_status_hash_tags(status_to_be_removed)
			self.update_hash_tag_count(hash_tags)


if __name__ == "__main__":
	auth = tweepy.OAuthHandler("JvNlID4knQzndSL1KYrAyWcCZ", "hv8ATIglBw6tW0BpCiosgr3K3zj2cCzZITn49YpAd4VPPUOJ3W")
	auth.set_access_token("1122274974092255233-O45AaTNC9LJEaXdtN9Eo3jFk97qhRq", "35QCR0JV6I3leLPXpi4cRE7PRBqS73lejHlc5WUopUXcg")
	api = tweepy.API(auth)

	myStreamListener = MyStreamListener()
	myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
	myStream.filter(track=['trump'])

