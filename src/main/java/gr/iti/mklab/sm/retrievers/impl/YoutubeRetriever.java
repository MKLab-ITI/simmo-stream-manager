package gr.iti.mklab.sm.retrievers.impl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import gr.iti.mklab.simmo.impl.posts.YoutubePost;
import gr.iti.mklab.simmo.impl.users.YoutubeAccount;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.google.api.services.youtube.model.Video;
import com.google.gdata.client.youtube.YouTubeQuery;
import com.google.gdata.client.youtube.YouTubeService;
import com.google.gdata.data.Link;
import com.google.gdata.data.media.mediarss.MediaDescription;
import com.google.gdata.data.media.mediarss.MediaPlayer;
import com.google.gdata.data.media.mediarss.MediaThumbnail;
import com.google.gdata.data.youtube.UserProfileEntry;
import com.google.gdata.data.youtube.VideoEntry;
import com.google.gdata.data.youtube.VideoFeed;
import com.google.gdata.data.youtube.YouTubeMediaContent;
import com.google.gdata.data.youtube.YouTubeMediaGroup;
import com.google.gdata.data.youtube.YtStatistics;
import com.google.gdata.util.ServiceException;

import gr.iti.mklab.simmo.core.UserAccount;
import gr.iti.mklab.simmo.core.documents.Post;
import gr.iti.mklab.sm.Credentials;
import gr.iti.mklab.sm.feeds.AccountFeed;
import gr.iti.mklab.sm.feeds.GroupFeed;
import gr.iti.mklab.sm.feeds.KeywordsFeed;
import gr.iti.mklab.sm.retrievers.Response;
import gr.iti.mklab.sm.retrievers.SocialMediaRetriever;

/**
 * Class responsible for retrieving YouTube content based on keywords and YouTube users
 * The retrieval process takes place through Google API
 *
 * @author ailiakop
 * @deprecated because it uses the YouTube Data API v2, which is obsolete
 * use {@link gr.iti.mklab.sm.retrievers.impl.YouTubeRetriever} instead
 */
@Deprecated
public class YoutubeRetriever extends SocialMediaRetriever {

    private final String activityFeedUserUrlPrefix = "http://gdata.youtube.com/feeds/api/users/";
    private final String activityFeedVideoUrlPrefix = "http://gdata.youtube.com/feeds/api/videos";
    private final String uploadsActivityFeedUrlSuffix = "/uploads";

    private Logger logger = Logger.getLogger(YoutubeRetriever.class);
    private boolean loggingEnabled = false;

    private YouTubeService service;

    public YoutubeRetriever(Credentials credentials) throws Exception {
        super(credentials);

        if (credentials.getClientId() == null || credentials.getKey() == null) {
            logger.error("YouTube requires authentication.");
            throw new Exception("YouTube requires authentication.");
        }

        this.service = new YouTubeService(credentials.getClientId(), credentials.getKey());
    }


    @Override
    public Response retrieveAccountFeed(AccountFeed feed, Integer maxRequests) {

        Response response = new Response();
        List<Post> posts = new ArrayList<Post>();

        Date lastItemDate = feed.getSinceDate();
        String label = feed.getLabel();

        boolean isFinished = false;

        String uName = feed.getUsername();

        int numberOfRequests = 0;

        if (uName == null) {
            logger.error("#YouTube : No source feed");
            return response;
        }

        UserAccount streamUser = getStreamUser(uName);
        if (loggingEnabled)
            logger.info("#YouTube : Retrieving User Feed : " + uName);

        URL channelUrl = null;
        try {
            channelUrl = getChannelUrl(uName);
        } catch (MalformedURLException e) {
            logger.error("#YouTube Exception : " + e.getMessage());
            return response;
        }

        while (channelUrl != null) {

            try {
                VideoFeed videoFeed = service.getFeed(channelUrl, VideoFeed.class);
                //service.getEntry(channelUrl, UserProfileEntry.class);
                numberOfRequests++;

                for (VideoEntry video : videoFeed.getEntries()) {

                    com.google.gdata.data.DateTime publishedTime = video.getPublished();
                    DateTime publishedDateTime = new DateTime(publishedTime.toString());
                    Date publicationDate = publishedDateTime.toDate();

                    if (publicationDate.after(lastItemDate) && (video != null && video.getId() != null)) {
                        /*
                        YoutubeVideo ytItem = new YoutubeVideo(video);
						ytItem.setList(label);
						
						if(streamUser != null) {
							ytItem.setUserId(streamUser.getId());
							ytItem.setStreamUser(streamUser);
						}
						
						items.add(ytItem);
						*/

                    }

                    if (numberOfRequests > maxRequests) {
                        isFinished = true;
                        break;
                    }

                }

                if (isFinished)
                    break;

                Link nextLink = videoFeed.getNextLink();
                channelUrl = nextLink == null ? null : new URL(nextLink.getHref());

            } catch (Exception e) {
                logger.error("#YouTube Exception : " + e.getMessage());
                break;
            }

        }

        if (loggingEnabled) {
            logger.info("#YouTube : Handler fetched " + posts.size() + " videos from " + uName +
                    " [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
        }

        response.setRequests(numberOfRequests);
        response.setPosts(posts);
        return response;
    }

    @Override
    public Response retrieveKeywordsFeed(KeywordsFeed feed, Integer maxRequests) throws Exception {

        Response response = new Response();
        List<Post> posts = new ArrayList<Post>();

        Date lastItemDate = feed.getSinceDate();
        String label = feed.getLabel();

        int startIndex = 1;
        int numPerPage = 25;
        int currResults = 0;
        int numberOfRequests = 0;

        boolean isFinished = false;

        List<String> keywords = feed.getKeywords();

        if (keywords == null || keywords.isEmpty()) {
            logger.error("#YouTube : No keywords feed");
            return response;
        }

        String tags = "";
        for (String key : keywords) {
            String[] words = key.split(" ");
            for (String word : words) {
                if (!tags.contains(word) && word.length() > 1)
                    tags += word.toLowerCase() + " ";
            }
        }

        //one call - 25 results
        if (tags.equals(""))
            return response;

        YouTubeQuery query;
        try {
            query = new YouTubeQuery(new URL(activityFeedVideoUrlPrefix));
        } catch (MalformedURLException e1) {

            return response;
        }

        query.setOrderBy(YouTubeQuery.OrderBy.PUBLISHED);
        query.setFullTextQuery(tags);
        query.setSafeSearch(YouTubeQuery.SafeSearch.NONE);
        query.setMaxResults(numPerPage);

        VideoFeed videoFeed = new VideoFeed();

        while (true) {
            try {
                query.setStartIndex(startIndex);
                videoFeed = service.query(query, VideoFeed.class);

                numberOfRequests++;

                currResults = videoFeed.getEntries().size();
                startIndex += currResults;

                for (VideoEntry video : videoFeed.getEntries()) {
                    com.google.gdata.data.DateTime publishedTime = video.getPublished();
                    DateTime publishedDateTime = new DateTime(publishedTime.toString());
                    Date publicationDate = publishedDateTime.toDate();

                    if (publicationDate.after(lastItemDate) && (video != null && video.getId() != null)) {

                        YoutubePost ytItem = new YoutubePost(video);
                        //ytItem.setList(label);

						/*UserAccount tempStreamUser = ytItem.getContributor();
						if(tempStreamUser != null) {
                            UserAccount user = this.getStreamUser(tempStreamUser);
							if(user != null) {
								ytItem.setUserId(user.getId());
								ytItem.setStreamUser(user);
							}
						}*/
                        posts.add(ytItem);
                    }
                }
                if (numberOfRequests >= maxRequests) {
                    isFinished = true;
                    break;
                }

            } catch (Exception e) {
                e.printStackTrace();
                logger.error("YouTube Retriever error during retrieval of " + tags);
                logger.error("Exception: " + e.getMessage());
                break;
            }

            if (isFinished)
                break;
        }

        if (loggingEnabled) {
            logger.info("#YouTube : Handler fetched " + posts.size() + " videos from " + tags +
                    " [ " + lastItemDate + " - " + new Date(System.currentTimeMillis()) + " ]");
        }

        Date dateToRetrieve = new Date(System.currentTimeMillis() - (24 * 3600 * 1000));
        feed.setSinceDate(dateToRetrieve);

        response.setRequests(numberOfRequests);
        response.setPosts(posts);
        return response;
    }

    @Override
    public Response retrieveGroupFeed(GroupFeed feed, Integer maxRequests) {
        return new Response();
    }

    public void stop() {
        if (service != null) {
            service = null;
        }
    }

    private URL getChannelUrl(String channel) throws MalformedURLException {
        StringBuffer urlStr = new StringBuffer(activityFeedUserUrlPrefix);
        urlStr.append(channel).append(uploadsActivityFeedUrlSuffix);

        return new URL(urlStr.toString());
    }

    public Video getMediaItem(String id) {
        try {
            URL entryUrl = new URL(activityFeedVideoUrlPrefix + "/" + id);
            VideoEntry entry = service.getEntry(entryUrl, VideoEntry.class);
            if (entry != null) {
                YouTubeMediaGroup mediaGroup = entry.getMediaGroup();
                List<YouTubeMediaContent> mediaContent = mediaGroup.getYouTubeContents();
                List<MediaThumbnail> thumbnails = mediaGroup.getThumbnails();

                String videoURL = null;
                for (YouTubeMediaContent content : mediaContent) {
                    if (content.getType().equals("application/x-shockwave-flash")) {
                        videoURL = content.getUrl();
                        break;
                    }
                }

                if (videoURL != null) {
                    MediaPlayer mediaPlayer = mediaGroup.getPlayer();
                    YtStatistics statistics = entry.getStatistics();

                    Long publicationTime = entry.getPublished().getValue();

                    String mediaId = "Youtube#" + mediaGroup.getVideoId();
                    URL url = new URL(videoURL);

                    String title = mediaGroup.getTitle().getPlainTextContent();

                    MediaDescription desc = mediaGroup.getDescription();
                    String description = desc == null ? "" : desc.getPlainTextContent();
                    //url
                    Video video = null;//new YoutubeVideo(url);
					
					/*
					//id
					mediaItem.setId(mediaId);
					//SocialNetwork Name
					mediaItem.setSource("Youtube");
					//Type 
					mediaItem.setType("video");
					//Time of publication
					mediaItem.setPublicationTime(publicationTime);
					//PageUrl
					String pageUrl = mediaPlayer.getUrl();
					mediaItem.setPageUrl(pageUrl);
					//Thumbnail
					MediaThumbnail thumb = null;
					int size = 0;
					for(MediaThumbnail thumbnail : thumbnails) {
						int t_size = thumbnail.getHeight() * thumbnail.getWidth();
						if(t_size > size) {
							thumb = thumbnail;
							size = t_size;
						}
					}
					//Title
					mediaItem.setTitle(title);
					mediaItem.setDescription(description);
					
					//Popularity
					if(statistics!=null){
						mediaItem.setLikes(statistics.getFavoriteCount());
						mediaItem.setViews(statistics.getViewCount());
					}
					Rating rating = entry.getRating();
					if(rating != null) {
						mediaItem.setRatings(rating.getAverage());
					}
					//Size
					if(thumb!=null) {
						mediaItem.setThumbnail(thumb.getUrl());
						mediaItem.setSize(thumb.getWidth(), thumb.getHeight());
					}
					
					String uploader = mediaGroup.getUploader();
					StreamUser user = getStreamUser(uploader);
					if(user != null) {
						mediaItem.setUser(user);
						mediaItem.setUserId(user.getId());
					}
					*/

                    return video;
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    @Override
    public UserAccount getStreamUser(String uid) {
        URL profileUrl;
        try {
            profileUrl = new URL(activityFeedUserUrlPrefix + uid);
            UserProfileEntry userProfile = service.getEntry(profileUrl, UserProfileEntry.class);

            UserAccount user = new YoutubeAccount(userProfile);

            return user;
        } catch (MalformedURLException e) {
            //e.printStackTrace();
            logger.error(e.getMessage());
        } catch (IOException e) {
            //e.printStackTrace();
            logger.error(e.getMessage());
        } catch (ServiceException e) {
            //e.printStackTrace();
            logger.error(e.getMessage());
        }

        return null;
    }

}
