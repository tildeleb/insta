// Copyright © 2012-2014 Lawrence E. Bakst. All rights reserved.

// This code is a simulaton model of Instagram and maybe Twitter too

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
	p := &post{pid: postID, text: tweet, fingerprint: hash(tweet), timestamp: t, deleted: false}
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
	text        string
	fingerprint uint64
	timestamp   int64 // "unix" format
	deleted     bool
}

type server struct {
	intance int
}

type imageServer struct {
	server
}

type workerServer struct {
	server
}

func view(uid int) {
	//fmt.Printf("view: uid=%d\n", uid)
	user := getUser(uid)
	for i, id := range user.following {
		f := getUser(id)
		fmt.Printf("view: user %q followng %q\n", user.name, f.name)
		ts := user.timestamps[i]
		if f.timestamp == ts {
			fmt.Printf("no new posts from %q\n", f.name)
			continue
		}
		posts := f.postsAfter(ts)
		for _, post := range posts {
			fmt.Printf("%q posted post P%d contains %q\n", f.name, post.pid, post.text)
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
	makeUsers(5)
	ts := makeUser("Taylor Swift", 3)
	es := makeUser("Ed Sheeran", 5)
	ts.post("foo")
	ts.post("bar")
	es.post("baz")
	fmt.Printf("ts=%#v\n", ts)
	fmt.Printf("es=%#v\n", es)
	for _, f := range followers {
		fmt.Printf("%q=%#v\n", f.name, f)
	}
	for _, user := range users {
		view(user.uid)
	}
}
