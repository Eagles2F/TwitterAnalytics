package io.purrito.big;

public class Tweet
{
    private String userId;
    private String tweetId;
    private String time;
    private String text;
    private int score;

    public Tweet() {
    }

    public Tweet(String userId, String tweetId, String time, String text, int score) {
        this.userId = userId;
        this.tweetId = tweetId;
        this.time = time;
        this.text = text;
        this.score = score;
    }
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
    public String getTweetId() {
        return tweetId;
    }

    public void setTweetId(String tweetId) {
        this.tweetId = tweetId;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }
}
