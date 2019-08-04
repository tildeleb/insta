// Copyright © 2012-2014 Lawrence E. Bakst. All rights reserved.

// This code is a simulaton model of Instagram and maybe Twitter too
// I didn't  discuss or goggle anythibg important
// The only thing I googled where so stats about Insta and other high volume users besides Taylor Swift

// Insta facts
// about 1 billion users or 1 Gusers
// They say 500M users are active each day, but that seems high to me
// unlimited followers
// max 7500 following
// avg followers 150
// 95 million posts are made every day (3 years ago data point) call it 100M posts/day
// As of 2018, there have been over 45 billion photos shared on this platform
// A study has discovered that 8% of all Instagram accounts are fake
// It’s estimated that posts that include at least one hashtag gain 12.6% more engagement
// More than 4.2 billion number of Instagram likes per day
// Around 95 millions photos is uploaded per a single day
// what is the latency from post to first view? can't find it
// it appears their post notification system is kinda broken

// Top stars by number of  followers — as of January 2019.
// Cristiano Ronaldo 150 M
// Selena Gomez 144 M
// Ariana Grande 142 M
// Dwayne 'The Rock' Johnson 127 M
// Kim Kardashian 124 M
// Kylie Jenner 123 M
// Beyoncé 122 M
// Taylor Swift M

// Not considered
// likes, saved
// hashtags
// different post types
// stories
// ads
// non chronological order timeline

// Assume
// The main operation is a user viewing their timeline, we assume chronological order
// assume 3 kinds of servers
// DB, obviously sharded
// Image, servers
// Workers, to resolve views
// users are bimodal so partition users into two groups based on a load factor thresthold
// user load factor is followers * posts per day * engagement
// cache goal is to reduce load on DB and image servers
// 2 level caching, in memory on workers and SSD on image and DB servers, eg avoid hitting DB and S3 for images
// fields we need from DB are current timestamp of large users and their most recent posts and their associated timestamps
// posting doesn't mean much if no users try to view, it's inherently lazy, which is a huge plus

// Solution
// I can't simulate 1GUsers on my laptop so I will reduce everything by 1000 to 1M users
// a user calls view and we first determine which users have posted since we last viewed
// it's probably a small percentage of the followers and followers are capped at 7,500
// so then we scan the active followers and looks for the oldest timestamp, drop when we
// reach what we have already displayed and repeat until there

/*
given n users and m workers assign n/m users to each server to simulate a load balancer
some percentage "pu" of the users assigned to each server view insta every "ws" wakeup seconds
*/

package main

import (
	"fmt"
	"time"

	"leb.io/hashland/jenkins"
)

type user struct {
	name       string
	uid        int
	timestamp  int64 // timestamp of latest post
	following  []int
	timestamps []int64 // parallel array to following, timestamp of last post of user we are following
	followers  []int
	posts      []*post
}

// user u follows user f
// user f is a follower of user u
func (u *user) follow(f *user) {
	f.followers = append(f.followers, u.uid)
	u.following = append(u.following, f.uid)
	u.timestamps = append(u.timestamps, f.timestamp)
}

var lastTime int64
var postID int

func (u *user) post(tweet string) {
	t := time.Now().UnixNano()
	if t == lastTime {
		fmt.Printf("bump\n")
		t++
		lastTime = t
	}
	p := &post{pid: postID, uid: u.uid, text: tweet, fingerprint: hash(tweet), timestamp: t, deleted: false}
	fmt.Printf("post: pid=%d, ts=%d, text=%q\n", p.pid, p.timestamp, p.text)
	u.timestamp = t
	postID++
	u.posts = append(u.posts, p)
}

// return all the posts that happened after ts
func (u *user) postsAfter(ts int64) (posts []*post) {
	for _, post := range u.posts {
		if post.timestamp > ts && !post.deleted {
			posts = append(posts, post)
		}
	}
	return
}

// id is the index into users
var uid = 0
var users []*user
var followers []*user

func getUser(uid int) *user {
	return users[uid]
}

type post struct {
	pid         int
	uid         int
	text        string
	fingerprint uint64
	timestamp   int64 // "unix" format
	deleted     bool
}

type View struct {
	uid int
	rid int
}

type server struct {
	msgs    chan interface{}
	intance int
}

type imageServer struct {
	server
}

type workerServer struct {
	server
}

var ws0 workerServer

func (ws *workerServer) start() {
	for {
		msg := <-ws.msgs
		switch msg.(type) {
		case *View:
			//v := msg.(*View)
		case *post:
			//p := msg.(*post)
		}
	}
}

func view(uid int) {
	//fmt.Printf("view: uid=%d\n", uid)
	var aposts [][]*post
	var users = make(map[int]*user)

	user := getUser(uid)

	// find all users that have new posts and get the posts we haven't seen
	for i, id := range user.following {
		f := getUser(id)
		users[id] = f
		fmt.Printf("view: user %q followng %q\n", user.name, f.name)
		ts := user.timestamps[i]
		if f.timestamp == ts {
			fmt.Printf("no new posts from %q\n", f.name)
			continue
		}
		aposts = append(aposts, f.postsAfter(ts))
	}

	// the posts are sorted so merge them into a chronological timeline
	for {
		cnt := 0
		for i, posts := range aposts {
			if len(posts) != 0 {
				p := aposts[i][0]
				fmt.Printf("%q posted post P%d contains %q, ts=%d\n", users[p.uid].name, p.pid, p.text, p.timestamp)
				aposts[i] = aposts[i][1:]
				cnt++
			}
		}
		if cnt == 0 {
			break
		}
	}
}

func newUser(name string) *user {
	u := &user{name: name, uid: uid}
	uid++
	users = append(users, u)
	return u
}

var h = jenkins.New364(0)

func hash(s string) (ret uint64) {
	h.Write([]byte(s))
	ret = h.Sum64()
	h.Reset()
	return
}

func makeUsers(n int) {
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("follower%d", i)
		followers = append(followers, newUser(name))
	}
}

func makeUser(name string, nfollowers int) *user {
	user := newUser(name)
	for i := 0; i < nfollowers; i++ {
		u := followers[i]
		u.follow(user)
	}
	return user
}

func main() {
	makeUsers(1000000)
	ts := makeUser("Taylor Swift", 129000)
	es := makeUser("Ed Sheeran", 50000)
	ts.post("foo")
	es.post("bar")
	ts.post("baz")
	es.post("quux")
	//fmt.Printf("ts=%#v\n", ts)
	//fmt.Printf("es=%#v\n", es)
	//for _, f := range followers {
	//	fmt.Printf("%q=%#v\n", f.name, f)
	//}
	for _, user := range users {
		view(user.uid)
	}
}

/*
(*
  S has items to sample, R will contain the result
 *)
ReservoirSample(S[1..n], R[1..k])
  // fill the reservoir array
  for i = 1 to k
      R[i] := S[i]

  // replace elements with gradually decreasing probability
  for i = k+1 to n
    j := random(1, i)   // important: inclusive range
    if j <= k
        R[j] := S[i]

*/
