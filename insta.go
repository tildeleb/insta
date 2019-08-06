// Copyright © 2019 Lawrence E. Bakst. All rights reserved.

// Simulate a service like Instagram or Twitter

// Insta facts
// about 1 billion users or 1 Gusers
// 500M users are active each day, but that seems high to me
// unlimited followers
// max 7500 following
// avg followers 150
// 95 million posts are made every day (3 years ago data point) call it 100M posts/day
// as of 2018, there have been over 45 billion photos shared on this platform
// a study has discovered that 8% of all Instagram accounts are fake
// it's estimated that posts that include at least one hashtag gain 12.6% more engagement
// more than 4.2 billion number of Instagram likes per day
// around 95 millions photos is uploaded per a single day
// what is the latency from post to first view? can't find it
// it appears their post notification system is kinda broken

// Top stars by number of followers — as of January 2019.
// Cristiano Ronaldo 150 M
// Selena Gomez 144 M
// Ariana Grande 142 M
// Dwayne 'The Rock' Johnson 127 M
// Kim Kardashian 124 M
// Kylie Jenner 123 M
// Beyoncé 122 M
// Taylor Swift 120 M

// Not considered
// likes, saved
// hashtags
// different post types
// stories
// ads
// non chronological order timeline

// Assumptions and Approach
// posting doesn't require much resource if no followers view it, it's inherently lazy, which is a huge plus
// The main operations are:
// 1. a user viewing their timeline, we assume chronological order
// 2. a user making a post(s)
// 3. follow or unfollow
// 4. delete a post
// 3 kinds of instances:
// 1. Workers, to handle requests from users to either view their timeline or make a post
// 2. DB, stores data about users and posts
// 3. Image, maps hash of picture to url and has cache of popular and recent images
// users are bimodal so perhaps partition users into two groups based on a load factor threshold
// user load factor is followers * posts per day * views per day
// cache goal is to reduce load on DB, image, and worker servers
// 2 level caching, in memory and SSD on worker, image and DB servers, eg avoid hitting DB and S3
// fields we need from DB are current timestamp of large users and their most recent posts and their associated timestamps

// Solution
// I can't simulate 1GUsers on my laptop so I will reduce everything by 1000 to 1M users
// A user calls view. First determine which users have posted since the last view
// by comparing a saved timestamp of the last post viewed for each user they are following
// to the timestamp for the most recent post.
// It's probably a small percentage of the users they following and following is capped at 7,500.
// Scan the following that has posted and look for a matching timestamp and advance by one.
// Since the posts are sorted chronologically we do a merge of all the following posts to get the posts
// in chronological order.

// Todo
// deleted posts
// folowing and unfollowing
// no multiple worker instances
// no image servers
// DB not modeled as a server yet
// no graph of users
// Still doesn't accurately simulate how a scale Insta could really work
// shard users into segments and distribute segments to multiple workers
// Caching of images and tweets at several levels

// Given n users and m workers assign n/m users to each server to simulate a load balancer
// Some percentage "pu" of the users assigned to each server view insta every "ws" wakeup seconds
// Use reservoir sampling to generate segments of users?

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"leb.io/hashland/jenkins"
)

// globals
var s = rand.NewSource(time.Now().UTC().UnixNano())
var r = rand.New(s)
var wg sync.WaitGroup
var ws0 workerServer
var h = jenkins.New364(0)
var db DB

// rbetween returns random int [a, b]
func rbetween(r *rand.Rand, a int, b int) int {
	return r.Intn(b-a+1) + a
}

func hash(s string) (ret uint64) {
	h.Write([]byte(s))
	ret = h.Sum64()
	h.Reset()
	return
}

// msg is what is sent to worker instances
type msg struct {
	op    string
	id    int
	tweet string
}

// operations that users can perform
const (
	POST = "post" // post a tweet or picture
	VIEW = "view" // view new posts
)

// represents the database entry for a user, think of the slices as columns sorted on timestamp
// no keys yet, just linear search
type user struct {
	name       string
	uid        int
	timestamp  int64 // timestamp of latest post
	followers  []int
	following  []int
	timestamps []int64 // parallel array to following, timestamp of last post of user we are following
	posts      []*post
}

// user u follows user f
// user f is a follower of user u
func (u *user) follow(f *user) {
	f.followers = append(f.followers, u.uid)
	u.following = append(u.following, f.uid)
	u.timestamps = append(u.timestamps, f.timestamp)
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

func (u *user) sendTimelineToUser(timeline []*post) {
	//fmt.Printf("view: timeline=%v\n", timeline)
}

type post struct {
	pid         int
	uid         int
	text        string
	fingerprint uint64
	timestamp   int64 // "unix" format
	deleted     bool
}

type DB struct {
	sync.Mutex
	uid       int
	lastTime  int64
	postID    int
	users     []*user
	followers []*user // used by sim for now
}

// assumes lock is held
func (db *DB) GetUser(uid int) *user {
	//fmt.Printf("GetUser: uid=%d, len(users)=%d\n", uid, len(users))
	return db.users[uid]
}

// assumes lock is held
func (db *DB) UpdateTimestamp(uid int, timestamp int64) {
	u := db.GetUser(uid)
	u.timestamp = timestamp
}

// NewUser allocates and stores a new user in the DB
// assumes lock is held
func (db *DB) NewUser(name string) *user {
	u := &user{name: name, uid: db.uid}
	db.uid++
	db.users = append(db.users, u)
	return u
}

func (db *DB) GetPostsAfter(uid int, timestamp int64) (posts []*post) {
	db.Lock()
	defer db.Unlock()
	u := db.GetUser(uid)
	posts = u.postsAfter(timestamp)
	return
}

func (db *DB) NumberOfUsers() int {
	db.Lock()
	defer db.Unlock()
	return len(db.users)
}

// MakeUsers makes users who are just followers (for now)
func (db *DB) MakeUsers(n int) []*user {
	db.Lock()
	defer db.Unlock()
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("follower%d", i)
		db.followers = append(db.followers, db.NewUser(name))
	}
	return db.followers
}

// MakeUser makes famous users who have lots of followers
func (db *DB) MakeUser(name string, nfollowers int) *user {
	db.Lock()
	defer db.Unlock()
	user := db.NewUser(name)
	for i := 0; i < nfollowers; i++ {
		u := db.followers[i]
		u.follow(user)
	}
	return user
}

func (db *DB) Post(uid int, tweet string) {
	db.Lock()
	defer db.Unlock()
	u := db.GetUser(uid)
	t := time.Now().UnixNano()
	if t == db.lastTime {
		fmt.Printf("bump\n")
		t++
		db.lastTime = t
	}
	p := &post{pid: db.postID, uid: u.uid, text: tweet, fingerprint: hash(tweet), timestamp: t, deleted: false}
	tim := time.Unix(0, p.timestamp)
	fmt.Printf("Post: ts=%v, pid=P%d, name=%q, text=%q\n", tim, p.pid, u.name, p.text)
	db.UpdateTimestamp(u.uid, t)
	//u.timestamp = t // call update timestamp
	db.postID++
	u.posts = append(u.posts, p)
}

// View returns all the new posts on a users timeline in chronological order
func (db *DB) View(uid int) (timeline []*post) {
	var aposts [][]*post
	var ids []int
	var users = make(map[int]*user)
	var t1, t2 time.Time

	db.Lock()
	defer db.Unlock()
	u := db.GetUser(uid)

	// find all users that have new posts and get the posts we haven't seen
	for i, id := range u.following {
		f := db.GetUser(id)
		users[id] = f
		fmt.Printf("View: user %q followng %q\n", u.name, f.name)
		ts := u.timestamps[i]
		if f.timestamp == ts {
			fmt.Printf("View: no new posts from %q\n", f.name)
			continue
		}
		posts := f.postsAfter(ts)
		fmt.Printf("View: %d new posts from %q\n", len(posts), f.name)
		u.timestamps[i] = posts[len(posts)-1].timestamp // update DB
		aposts = append(aposts, posts)
		ids = append(ids, id)
	}

	// the posts and timestamps are sorted (by timestamp) so merge them into a chronological timeline
	if len(aposts) > 0 {
		for {
			cnt := 0
			idx := -1
			for i, posts := range aposts {
				if len(posts) != 0 {
					if cnt == 0 {
						t1 = time.Unix(0, aposts[i][0].timestamp)
						idx = i
					} else {
						t2 = time.Unix(0, aposts[i][0].timestamp)
						if t1.After(t2) {
							idx = i
							t1 = t2
						}
					}
					cnt++
				}
			}
			if cnt == 0 {
				break
			}
			p := aposts[idx][0]
			timeline = append(timeline, p)
			tim := time.Unix(0, p.timestamp)
			fmt.Printf("View: ts=%v, user %q saw that %q posted post P%d contains %q\n",
				tim, u.name, users[p.uid].name, p.pid, p.text)
			aposts[idx] = aposts[idx][1:]
		}
		fmt.Printf("\n")
	}
	return timeline
}

type View struct {
	uid int
	rid int
}

type server struct {
	instance int
}

type imageServer struct {
	server
}

type workerServer struct {
	server
}

type DBServer struct {
	server
}

// start processes posts and views from users
func (ws *workerServer) start(msgs chan msg) {
	fmt.Printf("worker %d starting\n", ws.instance)
	for {
		msg := <-msgs
		switch msg.op {
		case POST:
			u := db.GetUser(msg.id)
			db.Post(u.uid, msg.tweet)
		case VIEW:
			u := db.GetUser(msg.id)
			timeline := db.View(u.uid)
			u.sendTimelineToUser(timeline)
		}
	}
}

//
// code below here is very simple Instagram simulator
//

func tweeter(ch chan msg, stars []*star) {
	var s = rand.NewSource(time.Now().UTC().UnixNano())
	var r = rand.New(s)
	for {
		star := stars[rbetween(r, 0, len(stars)-1)]
		uid := star.user.uid
		switch rbetween(r, 1, 5) {
		case 5:
			ch <- msg{op: POST, id: uid, tweet: "foo"}
			fallthrough
		case 4:
			ch <- msg{op: POST, id: uid, tweet: "bar"}
			fallthrough
		case 3:
			ch <- msg{op: POST, id: uid, tweet: "baz"}
			fallthrough
		case 2:
			ch <- msg{op: POST, id: uid, tweet: "quux"}
			fallthrough
		case 1:
			ch <- msg{op: POST, id: uid, tweet: "foobar"}
		}
		time.Sleep(time.Duration(rbetween(r, 1, 2)) * time.Second)
	}
}

func viewer(ch chan msg) {
	var s = rand.NewSource(time.Now().UTC().UnixNano())
	var r = rand.New(s)
	idx := 0
	for {
		auser := rbetween(r, 0, db.NumberOfUsers()-1)
		u := db.GetUser(auser)
		ch <- msg{op: VIEW, id: u.uid, tweet: ""}
		u = db.GetUser(idx)
		ch <- msg{op: VIEW, id: u.uid, tweet: ""}
		idx++
		if idx > db.NumberOfUsers()-1 {
			idx = 0
		}
		time.Sleep(time.Duration(rbetween(r, 1, 2)) * time.Second)
	}
}

const maxUsers = 10

type star struct {
	name      string
	followers int
	user      *user
}

var stars = []*star{&star{"Taylor Swift", 5, nil}, &star{"Ed Sheeran", 3, nil}, &star{"Beyoncé", 7, nil}}

// simulate Instagram
func sim() {
	ch := make(chan msg, 100) // channel to communicate to worker instance
	go ws0.start(ch)
	db.MakeUsers(maxUsers) // make followers for stars

	// make a few stars
	for _, star := range stars {
		star.user = db.MakeUser(star.name, star.followers)
	}

	wg.Add(2)
	go tweeter(ch, stars)
	go viewer(ch)
	wg.Wait() // currently never terminates, use ^C
}

func main() {
	sim()
}
