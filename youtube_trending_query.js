//find all
db.youtube_trending.find()

//find videos with 5 "tags"
db.youtube_trending.find({tags : {$size: 5}})

//find video with comment count over 2000
db.youtube_trending.find({comment_count : {$gt : 2000}})
   .projection({})
   .sort({_id:-1})
   
//find video with over 200 000 likes and under 10M views
db.youtube_trending.find({likes : {$gt : 200000}, views : {$lt : 10000000}})
.projection({})
.sort({_id:-1})

//different tags in videos 
db.youtube_trending.distinct("tags")

//group by ratings disability and find the number of videos for each and sort dissending 
db.youtube_trending.aggregate(
    {$group: { _id: "$ratings_disabled", total:{$sum: 1}}},
    {$sort : {total : 1}}
)

//group by category titles and count videos 
db.youtube_trending.aggregate(
    {$group: { _id: "$category.snippet.title", total : {$sum : 1}}},
    {$sort : {total : -1}}
)


//the most common tag in videos with category people and blogs
db.youtube_trending.aggregate(
    {$match : {"category.snippet.title" : /People & Blogs/}},
    {$unwind : "$tags"},
    {$group: { _id: "$tags", total:{$sum : 1}}},
    {$sort : {total : -1}}
)

//calculate the average number of likes for videos with category entertainment 
db.youtube_trending.aggregate(
    {$match: {"category.snippet.title" : /Entertainment/}},
    {$group: { _id: "$category", average:{$avg: "$likes"}}}
)

//find how many videos does each channel have
db.youtube_trending.aggregate(
    {$group: { _id: "$channel_title", total : {$sum : 1}}},
    {$sort: {total : -1}}
)

//word that appears most often in description
db.youtube_trending.aggregate([
    { $project: { reci: { $split: ["$description", " "] } } },
    { $unwind: "$reci" },
    { $group: { _id: "$reci", broj: { $sum: 1 } } },
    { $sort: { broj: -1 } },
    {$skip : 50},
    {$limit : 20}
])

//map reduce : for every tag get average number of likes 
var mapF = function(){
    this.tags.forEach(a => {
        emit(a, this.likes)
    })
}

var mapR = function(key, value){
    return {"result":Array.avg(value)}
}

db.youtube_trending.mapReduce(mapF, mapR, {out : "avg_likes_mr"})
db.avg_likes_mr.find()

//map reduce: number of videos per channel that have disabled ratings 
var mapF = function(){
    if(this.ratings_disabled === "True"){
        emit(this.channel_title, 1)
    }
}

var mapRed = function(key, val){
    return val.length;
}

db.youtube_trending.mapReduce(mapF, mapRed, {out : "videos_in_2015"})
db.videos_in_2015.find()


//get all channels that have over 2 000 000 views & display total views per channel

//aggregate 
db.youtube_trending.aggregate(
    {$match: {"views" : {$gt : 2000000}}},
    {$group: { _id: "$channel_title", total : {$sum: "$views"}}},
    {$sort : {views : -1}}
)

//map reduce
var mapV = function(){
    if(this.views > 2000000){
        emit(this.channel_title, this.views)
    }
}

var mapRedu = function(key, value){
    return Array.sum(value)
}

db.youtube_trending.mapReduce(mapV, mapRedu, {out : "test"})
db.test.find()

//<<<<<<<QUERY FROM THE EXAM>>>>>>>
//for every category title find the number of likes and comments

var mapFunction = function(){
    var count = { likes : this.likes, comments : this.comments}
    emit(this.category.snippet.title, count)
}

var mapReduce = function(key, value){
    var likes = 0;
    var comments = 0;
    value.forEach(a => { 
        likes += a.likes
        comments += a.comments
    })
    
    var final = {sumLikes: likes, sumComments: comments}
    
    return final;    
}

db.youtube_trending.mapReduce(mapFunction, mapReduce, {out : "sum_likes_comments"})
db.sum_likes_comments.find()