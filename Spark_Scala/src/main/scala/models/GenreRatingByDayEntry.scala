package models

case class GenreRatingByDayEntry(genre: String,
                                 one_star_count: Long,
                                 two_star_count: Long,
                                 three_star_count: Long,
                                 four_star_count: Long,
                                 five_star_count: Long,
                                 day: Int,
                                 year_month: Long)
