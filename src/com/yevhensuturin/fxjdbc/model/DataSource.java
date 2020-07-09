package com.yevhensuturin.fxjdbc.model;

import javafx.beans.property.SimpleIntegerProperty;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.PropertyResourceBundle;

public class DataSource {
    public static final String DB_NAME = "music.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:C:\\Users\\h241705\\IdeaProjects\\JavaFXPlusJDBC\\" + DB_NAME;

    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;


    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTIST_ID = "_id";
    public static final String COLUMN_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;


    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final int  INDEX_SONG_ID = 1;
    public static final int  INDEX_SONG_TRACK = 2;
    public static final int  INDEX_SONG_TITLE = 3;
    public static final int  INDEX_SONG_ALBUM = 4;

    public static final int ORDER_BY_NONE = 1;
    public static final int ORDER_BY_ASC = 2;
    public static final int ORDER_BY_DESC = 3;

    public static final String QUERY_ALBUMS_BY_ARTIST_START =
            "SELECT " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " FROM " + TABLE_ALBUMS +
            " INNER JOIN " + TABLE_ARTISTS + " ON " +  TABLE_ALBUMS + "." +  COLUMN_ALBUM_ARTIST +
            " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +
            " WHERE " + TABLE_ARTISTS + "." +  COLUMN_ARTIST_NAME + " = \"";
    public static final String QUERY_ALBUMS_BY_ARTIST_SORT =
            " ORDER BY " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";

    public static final String QUERY_ALL_ARTISTS_START = "SELECT * FROM " + TABLE_ARTISTS;
    public static final String QUERY_ALL_ARTISTS_SORT = " ORDER BY " + COLUMN_ARTIST_NAME + " COLLATE NOCASE ";

    public static final String QUERY_ARTIST_FOR_SONG_START = "SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " + TABLE_SONGS + "." + COLUMN_SONG_TRACK +
            " FROM " + TABLE_SONGS +
            " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS + "." + COLUMN_SONG_ALBUM + "=" + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + "=" + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +
            " WHERE " + TABLE_SONGS + "." + COLUMN_SONG_TITLE + "=\"";
    public static final String QUERY_ARTIST_FOR_SONG_SORT = " ORDER BY " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";
    public static final String CREATE_ARTIST_FOR_SONG_VIEW =
            "CREATE VIEW IF NOT EXISTS " + TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
             TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONG_ALBUM + ", " + TABLE_SONGS + "." + COLUMN_SONG_TRACK + ", " +
             TABLE_SONGS + "." + COLUMN_SONG_TITLE +
            " FROM " + TABLE_SONGS +
            " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS + "." + COLUMN_SONG_ALBUM + "=" + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + "=" + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +
            " ORDER BY " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME +", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " +
                    TABLE_SONGS + "." + COLUMN_SONG_TRACK;

    public static final String QUERY_VIEW_SONG_INFO = "SELECT " + COLUMN_ARTIST_NAME + ", " + COLUMN_SONG_ALBUM + ", " + COLUMN_SONG_TRACK +
            " FROM " + TABLE_ARTIST_SONG_VIEW + " WHERE " + COLUMN_SONG_TITLE + " = \"";

    public static final String QUERY_VIEW_SONG_INFO_PREP = "SELECT " + COLUMN_ARTIST_NAME + ", " + COLUMN_SONG_ALBUM + ", " + COLUMN_SONG_TRACK +
            " FROM " + TABLE_ARTIST_SONG_VIEW + " WHERE " + COLUMN_SONG_TITLE + " = ?";

    public static final String INSERT_ARTIST = "INSERT INTO " + TABLE_ARTISTS + " (" + COLUMN_ARTIST_NAME + ") VALUES(?)";
    public static final String INSERT_ALBUMS = "INSERT INTO " + TABLE_ALBUMS + " (" + COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTIST + ") VALUES(?, ?)";
    public static final String INSERT_SONGS = "INSERT INTO " + TABLE_SONGS + " (" + COLUMN_SONG_TRACK + ", " + COLUMN_SONG_TITLE + ", " + COLUMN_SONG_ALBUM + ") VALUES(?, ?, ?)";

    public static final String QUERY_ARTISTS = "SELECT " + COLUMN_ARTIST_ID + " FROM " + TABLE_ARTISTS + " WHERE " + COLUMN_ARTIST_NAME + " = ?";
    public static final String QUERY_ALBUM = "SELECT " + COLUMN_ALBUM_ID + " FROM " + TABLE_ALBUMS + " WHERE " + COLUMN_ALBUM_NAME + " = ?";

    public static final String QUERY_ALBUMS_BY_ARTIST_ID = "SELECT * FROM " + TABLE_ALBUMS + " WHERE " + COLUMN_ALBUM_ARTIST + " = ? ORDER BY " +
            COLUMN_ALBUM_NAME + " COLLATE NOCASE";

    private Connection connection;

    private PreparedStatement querySongInfoView;
    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;

    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;
    private PreparedStatement queryAlbumsById;

    private static final DataSource instance = new DataSource();

    private DataSource(){}

    public static DataSource getInstance(){
        return instance;
    }

    public boolean open(){
        try{
            connection = DriverManager.getConnection(CONNECTION_STRING);
            querySongInfoView = connection.prepareStatement(QUERY_VIEW_SONG_INFO_PREP);
            insertIntoArtists = connection.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = connection.prepareStatement(INSERT_ALBUMS, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = connection.prepareStatement(INSERT_SONGS);
            queryArtist = connection.prepareStatement(QUERY_ARTISTS);
            queryAlbum = connection.prepareStatement(QUERY_ALBUM);
            queryAlbumsById = connection.prepareStatement(QUERY_ALBUMS_BY_ARTIST_ID);
            return true;
        } catch (SQLException e){
            System.out.println("Couldn't connect to the database " + e.getMessage());
            return false;
        }
    }

    public void close(){
        try {
            if(querySongInfoView != null){
                querySongInfoView.close();
            }

            if(insertIntoArtists != null){
                insertIntoArtists.close();
            }

            if(insertIntoAlbums != null){
                insertIntoAlbums.close();
            }

            if(insertIntoSongs != null){
                insertIntoSongs.close();
            }

            if(queryAlbum != null){
                queryAlbum.close();
            }

            if(queryArtist != null){
                queryArtist.close();
            }

            if(queryAlbumsById != null){
                queryAlbumsById.close();
            }

            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e){
            System.out.println("Couldn't close database " + e.getMessage());
        }
    }

    public List<Artist> queryArtists(int sortOrder){
        StringBuilder builder = new StringBuilder(QUERY_ALL_ARTISTS_START);
        if(sortOrder != ORDER_BY_NONE){
            builder.append(QUERY_ALL_ARTISTS_SORT);
            if(sortOrder == ORDER_BY_DESC){
                builder.append("DESC");
            }else{
                builder.append("ASC");
            }
        }

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(builder.toString())){

                List<Artist> artists = new ArrayList<>();
                while (results.next()) {
                    Artist artist = new Artist();
                    artist.setId(results.getInt(INDEX_ARTIST_ID));
                    artist.setName(results.getString(INDEX_ARTIST_NAME));
                    artists.add(artist);
                }

                return artists;
        }catch (SQLException e){
            System.out.println("Query failed: " + e.getMessage() );
            e.printStackTrace();
            return null;
        }
    }

    public List<String> queryAlbumsForArtist(String artistName, int sortOrder){
        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
        sb.append(artistName);
        sb.append("\"");

        if(sortOrder != ORDER_BY_NONE){
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if(sortOrder == ORDER_BY_DESC){
                sb.append("DESC");
            }else{
                sb.append("ASC");
            }
        }

        System.out.println("SQL statement: "+ sb.toString());

        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(sb.toString())){

            List<String> albums = new ArrayList<>();
            while (results.next()) {
                albums.add(results.getString(1));
            }
            return albums;
        }catch (SQLException e){
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
    }

    public void querySongsMetaDate(){
        String sql = "SELECT * FROM " + TABLE_ARTIST_SONG_VIEW;

        try (Statement statement = connection.createStatement();
             ResultSet results = statement.executeQuery(sql)){

            ResultSetMetaData meta = results.getMetaData();
            int numColumns = meta.getColumnCount();
            for(int i=1; i<=numColumns; i++){
                System.out.format("Column %d in the songs table is named %s\n", i, meta.getColumnName(i));
            }

        }catch (SQLException e){
            System.out.println("Query failed: " + e.getMessage());
        }
    }

    public int getCount(String table){
        String sql = "SELECT COUNT(*) AS count FROM " + table;

        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(sql)){

            int count = results.getInt("count");
            return count;
        }catch (SQLException e){
            System.out.println("Query failed: "+e.getMessage());
            return -1;
        }
    }

    public boolean createViewForSongArtists(){
        System.out.println(CREATE_ARTIST_FOR_SONG_VIEW);

        try(Statement statement = connection.createStatement()){
            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;
        }catch (SQLException e){
            System.out.println("Create View failed: "+e.getMessage());
            return false;
        }
    }

    private int insertArtist(String name) throws SQLException{
        queryArtist.setString(1, name);
        ResultSet results = queryArtist.executeQuery();
        if(results.next()){
            return results.getInt(1);
        } else {
            //Insert the artist
            insertIntoArtists.setString(1, name);
            int affectedRows = insertIntoArtists.executeUpdate();
            if(affectedRows != 1){
                throw new SQLException("Couldn't insert artists!");
            }

            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if(generatedKeys.next()){
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for artist");
            }
        }
    }

    private int insertAlbum(String name, int artistId) throws SQLException{
        queryAlbum.setString(1, name);
        ResultSet results = queryAlbum.executeQuery();
        if(results.next()){
            return results.getInt(1);
        } else {
            //Insert the album
            insertIntoAlbums.setString(1, name);
            insertIntoAlbums.setInt(2, artistId);
            int affectedRows = insertIntoAlbums.executeUpdate();
            if(affectedRows != 1){
                throw new SQLException("Couldn't insert album!");
            }

            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();
            if(generatedKeys.next()){
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Couldn't get _id for album");
            }
        }
    }

    public void insertSong(String title, String artist, String album, int track) {

        try{
            connection.setAutoCommit(false);

            int artistId = insertArtist(artist);
            int albumId = insertAlbum(album, artistId);

            insertIntoSongs.setInt(1, track);
            insertIntoSongs.setString(2, title);
            insertIntoSongs.setInt(3, albumId);

            int affectedRows = insertIntoSongs.executeUpdate();
            if(affectedRows == 1){
                  connection.commit();
            } else {
                throw new SQLException("The song insert failed!");
            }

        } catch (Exception e){
            System.out.println("Insert song exception" + e.getMessage());
            try {
                System.out.println("Performing rollback");
                connection.rollback();
            } catch (SQLException e2){
                System.out.println("Performing rollback exception: " + e2.getMessage());
            }
        } finally {
            try {
                System.out.println("Resetting default commit behavior");
                connection.setAutoCommit(true);
            } catch (SQLException e){
                System.out.println("Couldn't reset auto-commit! "+ e.getMessage());
            }
        }
    }

}
