package com.task

class Rating (var user_id: Integer, var movie_id: Integer, var rating:Integer) extends Serializable
  {
    override def toString: String = s"$movie_id has rating of $rating by user $user_id"
  }
