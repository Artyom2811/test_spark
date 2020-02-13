package models

case class Rating(userId: Long, movieId: Long, rating: Double, timestamps: Long)
