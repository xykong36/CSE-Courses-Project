import csv
import json
import time
import tweepy


# You must use Python 2.7.x
# Rate limit chart for Twitter REST API - https://dev.twitter.com/rest/public/rate-limits

def loadKeys(key_file):
    # TODO: put your keys and tokens in the keys.json file,
    #       then implement this method for loading access keys and token from keys.json
    # rtype: str <api_key>, str <api_secret>, str <token>, str <token_secret>

    # Load keys here and replace the empty strings in the return statement with those keys
    with open(key_file) as data: #use with to call close()
        keysdata = json.load(data) #load
    api_key = keysdata["api_key"]
    api_secret = keysdata["api_secret"]
    token = keysdata["token"]
    token_secret = keysdata["token_secret"]
    return api_key, api_secret, token, token_secret
    

# Q1.b.(i) - 5 points
def getPrimaryFriends(api, root_user, no_of_friends):
    # TODO: implement the method for fetching 'no_of_friends' primary friends of 'root_user'
    # rtype: list containing entries in the form of a tuple (root_user, friend)
    primary_friends = []
    friends_count = (api.get_user(root_user)).friends_count
    time.sleep(60)
    if (friends_count >= 20):
        root_user_friends = tweepy.Cursor(api.friends, screen_name = root_user).items(no_of_friends)
        time.sleep(60)
        for each in root_user_friends:
            primary_friends.append((root_user, each.screen_name))
    else:
        root_user_friends = tweepy.Cursor(api.friends, screen_name = root_user).items(friends_count)
        time.sleep(60)
        for each in root_user_friends:
            primary_friends.append((root_user, each.screen_name))
    return primary_friends

# Q1.b.(ii) - 7 points
def getNextLevelFriends(api, friends_list, no_of_friends):
    # TODO: implement the method for fetching 'no_of_friends' friends for each entry in friends_list
    # rtype: list containing entries in the form of a tuple (friends_list[i], friend)
    next_level_friends = []
    for i in range(len(friends_list)):
        temp_friend_list = getPrimaryFriends(api, friends_list[i][1], no_of_friends)
        next_level_friends.extend(temp_friend_list)
    return next_level_friends

# Q1.b.(iii) - 7 points
def getNextLevelFollowers(api, followers_list, no_of_followers):
    # TODO: implement the method for fetching 'no_of_followers' followers for each entry in followers_list
    # rtype: list containing entries in the form of a tuple (follower, followers_list[i])    
    next_level_followers = []
    # Add code here to populate next_level_followers
    for i in range(len(followers_list)):
        temp_followers_list = []
        user_name = followers_list[i][1]
        followers_count = (api.get_user(user_name)).followers_count
        time.sleep(60)
    #generate the temp_followers_list
        if(followers_count >= 20):
            root_user_followers = tweepy.Cursor(api.followers, screen_name = user_name).items(no_of_followers)
            time.sleep(60)
            for each in root_user_followers:
                temp_followers_list.append((each.screen_name, user_name))
            next_level_followers.extend(temp_followers_list)
                
        else:
            root_user_followers = tweepy.Cursor(api.followers, screen_name = user_name).items(followers_count)
            time.sleep(60)
            for each in root_user_followers:
                temp_followers_list.append((each.screen_name, user_name))
            next_level_followers.extend(temp_followers_list)
    return next_level_followers


# Q1.b.(i),(ii),(iii) - 4 points
def GatherAllEdges(api, root_user, no_of_neighbours):
    # TODO:  implement this method for calling the methods getPrimaryFriends, getNextLevelFriends
    #        and getNextLevelFollowers. Use no_of_neighbours to specify the no_of_friends/no_of_followers parameter.
    #        NOT using the no_of_neighbours parameter may cause the autograder to FAIL.
    #        Accumulate the return values from all these methods.
    # rtype: list containing entries in the form of a tuple (Source, Target). Refer to the "Note(s)" in the 
    #        Question doc to know what Source node and Target node of an edge is in the case of Followers and Friends. 
    all_edges = [] 
    #Add code here to populate all_edges
    Polo_primary_friends = getPrimaryFriends(api, root_user, no_of_neighbours)
    next_level_friends = getNextLevelFriends(api, Polo_primary_friends, no_of_neighbours)
    next_level_followers = getNextLevelFollowers(api, Polo_primary_friends, no_of_neighbours)
    all_edges.extend(Polo_primary_friends)
    all_edges.extend(next_level_friends)
    all_edges.extend(next_level_followers)
    return all_edges

# Q1.b.(i),(ii),(iii) - 5 Marks
def writeToFile(data, output_file):
   with open(output_file, 'w') as output:
        writer = csv.writer(output, delimiter = ',')
        writer.writerows(data)


"""
NOTE ON GRADING:

We will import the above functions
and use testSubmission() as below
to automatically grade your code.

You may modify testSubmission()
for your testing purposes
but it will not be graded.

It is highly recommended that
you DO NOT put any code outside testSubmission()
as it will break the auto-grader.

Note that your code should work as expected
for any value of ROOT_USER.
"""

def testSubmission():
    KEY_FILE = 'keys.json'
    OUTPUT_FILE_GRAPH = 'graph.csv'
    NO_OF_NEIGHBOURS = 20
    ROOT_USER = 'PoloChau'

    api_key, api_secret, token, token_secret = loadKeys(KEY_FILE)

    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(token, token_secret)
    api = tweepy.API(auth)

    edges = GatherAllEdges(api, ROOT_USER, NO_OF_NEIGHBOURS)
    try:
        time.sleep(5*60)
    except:
        TweepyError
        
    writeToFile(edges, OUTPUT_FILE_GRAPH)
    

if __name__ == '__main__':
    testSubmission()
    

