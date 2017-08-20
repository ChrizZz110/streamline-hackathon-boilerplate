package eu.streamline.hackathon.common.data;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.Serializable;

public class TwitEvent implements Serializable {

    public String created_at;
    public Long id;
    public String user;
    public String text;
    public String userScreenName;
    public int followersCount;
    public int friendsCount;
    public String lang;

    public static TwitEvent fromString(String s) throws JSONException {
        JSONObject json = new JSONObject(s);
        TwitEvent event = new TwitEvent();
        event.id = (Long)json.get("id");
        event.created_at = json.get("created_at").toString();
        event.text = (String) json.get("text");
        event.user = json.getJSONObject("user").getString("name");
        event.userScreenName = json.getJSONObject("user").getString("screen_name");
        event.followersCount = json.getJSONObject("user").getInt("followers_count");
        event.friendsCount = json.getJSONObject("user").getInt("friends_count");
        event.lang = json.getString("lang");
        return event;
    }

    @Override
    public String toString() {
        return "TweetEvent{" +
                "id=" + id +
                ", created_at=" + created_at  +
                ", text=" + text  +
                ", user=" + user  +
                ", userScreenName=" + userScreenName  +
                ", followersCount=" + followersCount  +
                ", friendsCount=" + friendsCount  +
                ", lang=" + lang  +
                '}';
    }
}
