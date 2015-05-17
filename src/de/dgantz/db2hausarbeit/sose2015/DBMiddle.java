package de.dgantz.db2hausarbeit.sose2015;

import java.sql.*;
import java.sql.ResultSet;
/**
 * Created by dgantz on 05.05.15.
 *
 * Diese Implementierung dient als Middleware zum Arbeiten mit einer SQL Datenbank.
 * Diese Klasse wird fuer Abfragen auf der Datanbank ueber JDBC (Java Database Connection) verwendet.
 *
 * Die implementierten Methoden in diesem Source Code sind nach den in der Datenbank verwendet Entitaeten
 * untergliedert und stellen die in der Aufgebanstellung verlangten Funktionen zur Verfuegung.
 *
 * @author Dominic Gantz // 1224729 // GruppenNr.:
 */

public class DBMiddle {


    //***********************************************************************************************
    // INSTANZEN
    //***********************************************************************************************

    // Instanzen fuer Datenbankverbindung
    private String host;            // Datenbank host
    private String username;        // Benutzername des Datenbank Accounts
    private String password;        // Passwort des Datenbank Accounts
    private Connection con;         // Variable fuer Datenbankverbindung
    private ResultSet result;       // Variable fuer Ergebnismenge


    //***********************************************************************************************
    // KONSTRUKTUR
    //***********************************************************************************************

    public DBMiddle(String host, String username, String password) {
        this.host = "jdbc:mysql://" + host;
        this.username = username;
        this.password = password;
    }


    //***********************************************************************************************
    // METHODEN
    //***********************************************************************************************

    /**
     * <p>Die Methode <b>checkForTable</b> ueberprueft ob eine Tabelle bereits vorhanden ist.
     * Sie wird in den anderen Methoden zum Erstellen von Datensaetzen aufgerufen, um ggf. zuerst
     * eine neue betreffende Tabelle zu erstellen.</p>
     * @param   tbl_name    vom Typ <b>String</b> um die jeweilige Tabelle zu suchen
     * @return  true        vom Typ <b>boolean</b> falls vorhanden, andernfalls <p>flase</p>
     * @throws SQLException
     */
    private boolean checkForTable(String tbl_name) throws SQLException {
        String name = tbl_name;
        DatabaseMetaData dbm = con.getMetaData();
        ResultSet tables = dbm.getTables(null, null, name, null);
        if(tables.next()) {
            return true;
        } else {
            return false;
        }
    }


    /**
     * <p>Die Methode <b>getConnection</b> stellt eine Verbindung zur Datenbank her.
     * </p>
     * @return con          vom Typ <b>Connection</b>
     * @throws SQLException Verbindung fehlgeschlagen
     */
    public Connection getConnection() throws SQLException {
        try {
            // Verbindunsgaufbau zu Datenbank
            con = DriverManager.getConnection(this.host, this.username, this.password);
            System.out.println("-----------------------------------------------------------------");
            System.out.println("Verbindung zur Datanbank erfolgreich");

        } catch (SQLException e) {
            System.out.println("-----------------------------------------------------------------");
            System.err.println("Verbindung zur Datenbank fehlgeschlagen.");
        }
        return con;
    }

    
    //----------------------------------------------------------------------------------------
    // Implementierung zum Bearbeiten der Tabelle "Raum"
    //----------------------------------------------------------------------------------------

    /**
     * <p>Die Methode <b>createRoom</b> dient zur Erstellung eines
     * neuen Datensatzes in der in der Ralation "Raum".
     * </p>
     * @param seats         vom Typ <b>int</b>
     * @return success      vom Typ <b>boolean</b> wenn Erstellung erfolgreich <p>ture</p>,
     *                      andernfalls <p>false</p>.
     */
    public boolean createRoom(int seats) {
        boolean success = false;
        //----------------------------------------------------------------------------------------
        // Verbindungsversuch zur Datenbank
        try {
            con = getConnection();
            System.out.println("Erstelle neuen Raum mit " + seats + " Sitzplätzen...\n");
            //----------------------------------------------------------------------------------------
            // SQL Anweisung zum erstellen eines neuen Raums
            try {
                // Ueberpruefung ob Tabelle "Raum" existiert
                if(!checkForTable("tbl_raum")) {
                    System.err.println("Die Tabelle Raum existiert nicht! Es wird eine neue Tabelle angelegt...");
                    // neue Tabelle Raum in Datenbank erstellen
                    PreparedStatement preSQL = con.prepareStatement("CREATE TABLE tbl_raum " +
                            "(raumNr INTEGER NOT NULL AUTO_INCREMENT, " +
                                "sitzplatz INTEGER NOT NULL, PRIMARY KEY (raumNr))");
                    preSQL.execute();                   // Anweisung zum Erstellen einer neuen Tabelle RAUM
                    System.out.println("Es wurde eine neue Tablle Raum erfolgreich erstellt.\n");
                }
                // Anweisung zum Einfuegen eines neuen Datensatzes
                PreparedStatement preSQL2 = con.prepareStatement("INSERT INTO tbl_raum (sitzplatz) VALUES(?)");
                preSQL2.setInt(1, seats);               // Parametersetzung fuer die vordefinierte Anweisung
                System.out.println("Datensatz wird erstellt...\n");
                preSQL2.execute();                      // Ausfuehrung der vordefinierten Anweisung

                success = true;
                System.out.println("Der Datensatz wurde erfolgreich erstellt.");

                con.close();                            // Verbindung zur Datenbank trennen
                System.out.println("Die Verbindung zur Datenbank wurde getrennt!");
                System.out.println("-----------------------------------------------------------------\n");
            } catch (SQLException e) {
                System.err.println("Der gewuenschte Datensatz konnte aufgrund eines Fehlers nicht ertellt werden.");
                e.printStackTrace();
            }
            //----------------------------------------------------------------------------------------
        } catch (SQLException e) {
            System.err.println("Die Verbindung zur Datenbank konnte nicht hergestellt werden...!");
            System.out.println("-----------------------------------------------------------------\n");
        }
        //----------------------------------------------------------------------------------------
        return success;
    }


    /**
     * <p>Die Methode <b>createRoom</b> dient zum Ausgeben aller
     * gespeicherten Datensaetze in der Ralation "Raum".
     * </p>
     * @return set          vom Typ <b>ResultSet</b> als Ergebnismenge
     */
    public ResultSet getAllRooms() {
        //----------------------------------------------------------------------------------------
        // Verbindungsversuch zur Datenbank
        try {
            con = getConnection();
            System.out.println("Suche alle gespeicherten Räume...\n");
            //----------------------------------------------------------------------------------------
            // SQL Anweisung zum Abfragen aller Raeume
            try {
                // Ueberpruefung ob Datensaetze in "Raum" existieren
                if(!checkForTable("tbl_raum")) {
                    throw new SQLException("Es existieren keine Datensaetze in der Tabelle Raum!");
                } else {
                    PreparedStatement preSQL = con.prepareStatement("SELECT * FROM tbl_raum");
                    System.out.println("Hier sind alle gespeicherten Räume: \n");
                    return result = preSQL.executeQuery();     // Ausfuehrung der vordefinierten Anweisung
                }
            } catch (SQLException e) {
                result = null;
            }
            //----------------------------------------------------------------------------------------
            con.close();                                // Verbindung zur Datenbank trennen
            System.out.println("Die Verbindung zur Datenbank wurde getrennt!");
            System.out.println("-----------------------------------------------------------------\n");
        } catch (SQLException e) {
            System.err.println("Es konnte keine Verbindung zur Datenbank hergestellt werden!");
            System.out.println("-----------------------------------------------------------------\n");
        }
        //----------------------------------------------------------------------------------------
        return result;
    }


    /**
     * <p>Die Methode <b>getRoomById</b> dient zur Abfrage eines Datensatzes in
     * der Tabelle "Raum" mittels der Angabe des Primaerschluessels.
     * </p>
     * @param id            vom Typ <b>int</b>
     * @return room         vom Typ <b>ResultSet</b> als Ergebnismenge
     */
    public ResultSet getRoomById(int id) {
        //----------------------------------------------------------------------------------------
        // Verbindungsversuch zur Datenbank
        try {
            con = getConnection();
            System.out.println("Es wird nach dem gewuenschten Raum gesucht...\n");
            //----------------------------------------------------------------------------------------
            // SQL Anweisung zum Abfragen eines Raums ueber dessen Primaerschluessel
            try {
                // Ueberpruefung ob Datensatz in "Raum" existiert
                if(!checkForTable("tbl_raum")) {
                    throw new SQLException("Der gesuchte Datensatz existiert nicht!");
                } else {
                    PreparedStatement preSQL = con.prepareStatement("SELECT * FROM tbl_raum WHERE raumNr = ?");
                    preSQL.setInt(1, id);
                    System.out.println("Hier ist Ihr gesuchter Raum:");
                    return result = preSQL.executeQuery();     // Ausfuehrung der vordef. Anweisung und der Ergebnismenge zuweisen
                }
            } catch (SQLException e) {
                result = null;
            }
            //----------------------------------------------------------------------------------------
            con.close();                                // Verbindung zur Datenbank trennen
            System.out.println("Die Verbindung zur Datenbank wurde getrennt!");
        } catch(SQLException e) {
            result = null;
        }
        //----------------------------------------------------------------------------------------
        return result;
    }


    /**
     * <p>Die Methode <b>editRoom</b> dient zur Aktualisierung der Daten eines Datensatzes
     * in der Tabelle "Raum" mittels der Angabe des Primaerschluessels zur Identifikation
     * des zu bearbeitetenden Raums und der Anzahl an Sitzen, welche geaendert werden sollen.
     * </p>
     * @param id            vom Typ <b>int</b>
     * @param seats         vom Typ <b>int</b>
     * @return success      vom Typ <b>boolean</b> wenn Aktualisierung erfolgreich <p>true</p>,
     *                      andernfalls <p>false</p>.
     */
    public boolean editRooom(int id, int seats) {
        boolean success;
        //----------------------------------------------------------------------------------------
        // Verbindungsversuch zur Datenbank
        try {
            con = getConnection();
            //----------------------------------------------------------------------------------------
            // SQL Anweisung zum Aendern eines Raums
            try {
                PreparedStatement preSQL = con.prepareStatement("UPDATE tbl_raum SET sitzplatz = ? WHERE raumNr = ?");
                preSQL.setInt(1, seats);                // Parametersetzung fuer die Anzahl der Sitzplaetze
                preSQL.setInt(2, id);                   // Parametersetzung fuer die RaumNr
                // Ueberpruefung ob Datensatz in "Raum" existiert
                if (!checkForTable("tbl_raum")) {
                    throw new SQLException("Der Datensatz existiert nicht!");
                } else {
                    System.out.println("Aktualisiere Datensatz...\n");
                    preSQL.execute();                   // Ausfuehrung der vordef. Anweisung
                    success = true;
                    System.out.println("Der Datensatz wurde erfolgreich aktualisiert.");
                }
            } catch (SQLException e) {
                success = false;
            }
            //----------------------------------------------------------------------------------------
            con.close();                               // Verbindung zur Datenbank trennen
            System.out.println("Die Verbindung zur Datenbank wurde getrennt!");
        } catch (SQLException e) {
            success = false;
        }
        //----------------------------------------------------------------------------------------
        return success;
    }


    /**
     * <p>Die Methode <b>deleteRoomById</b> dient zum Loeschen eines Datensatzes in der Tabelle
     * "RAUM" mittels der Angabe des Primaerschluessels.
     * </p>
     * @param id            vom Typ <b>int</b>
     * @return success      vom Typ <b>boolean</b> wenn Loeschung erfolgreich <p>true</p>,
     *                      andernfalls <p>false</p>.
     */
    public boolean deleteRoomById(int id) {
        boolean success;
        //----------------------------------------------------------------------------------------
        // Verbindungsversuch zur Datenbank
        try {
            con = getConnection();
            //----------------------------------------------------------------------------------------
            // SQL Anweisung zum Loeschen eines Raums
            try {
                PreparedStatement preSQL = con.prepareStatement("DELETE FROM tbl_raum WHERE raumNr = ?");
                preSQL.setInt(1, id);                  // Parametersetzung fuer die vordefinierte Anweisung
                // Ueberpruefung ob Datensatz in "Raum" existiert
                if(!checkForTable("tbl_raum")) {
                    throw new SQLException("Der zu loeschende Datensatz exisitiert nicht!");
                } else {
                    System.out.println("Lösche Datensatz...\n");
                    preSQL.execute();             // Ausfuehrung der vordef. Anweisung
                    success = true;
                    System.out.println("Der Datensatz wurde erfolgreich geloescht.");
                }
            } catch (SQLException e) {
                success = false;
            }
            //----------------------------------------------------------------------------------------
            con.close();                                // Verbindung zur Datenbank trennen
            System.out.println("Die Verbindung zur Datenbank wurde getrennt!");
        } catch (SQLException e) {
            success = false;
        }
        //----------------------------------------------------------------------------------------
        return success;
    }


    //----------------------------------------------------------------------------------------
    // Implementierung zum Bearbeiten der Tabelle "Vorlesung"
    //----------------------------------------------------------------------------------------

    /**
     * <p>Die Methode <b>createLecture</b> dient zur Erstellung einer neuen Vorlesung in der
     * Tabelle "Vorlesung"
     * </p>
     * @param name          vom Typ <b>String</b>
     * @param roomnr        vom Typ <b>int</b>
     * @param professornr   vom Typ <b>int</b>
     * @param coursenr      vom Typ <b>int</b>
     * @return success      vom Typ <b>boolean</b> wenn Erstellung erfolgreich <p>true</p>,
     *                      andernfalls <p>false</p>
     */
    public boolean createLecture(String name, int roomnr, int professornr, int coursenr) {
        boolean success = false;
        //----------------------------------------------------------------------------------------
        // Verbindungsversuch zur Datenbank
        try {
            con = getConnection();
            //----------------------------------------------------------------------------------------
            // SQL Anweisung zum erstellen einer neuen Vorlesung
            try {
                // Ueberpruefung ob Tabelle existiert
                DatabaseMetaData dbm = con.getMetaData();
                ResultSet tables = dbm.getTables(null, null, "tbl_raum", null);
                if(!tables.next()) {
                    System.err.println("Die Tabelle Vorlesung exisitiert nicht. Es wird eine neue Tabelle angelegt...\n");
                    PreparedStatement preSQL2 = con.prepareStatement("CREATE TABLE tbl_vorlesung " +
                            "(vorlesungsNr INTEGER NOT NULL AUTO_INCREMENT, raumNr INTEGER NOT NULL, " +
                            "name VARCHAR(30) NOT NULL, professorenNr INTEGER NOT NULL," +
                            "PRIMARY KEY(vorlesungsNr), FOREIGN KEY(raumNr)REFERENCES tbl_raum(raumNr)," +      // CONSTRAINT raumNr
                            "FOREIGN KEY(professorenNr) REFERENCES tbl_professor(professorenNr)");
                    preSQL2.executeQuery();
                    System.out.println("Es wurde eine neue Tabelle Vorlesung angelegt!\n");
                }
                PreparedStatement preSQL = con.prepareStatement("INSERT INTO Vorlesung " +
                        "(Name, raumNr, ProfessorenNr, VorlesungsNr)" +
                        "VALUES(?, ?, ?, ?)");
                // Parametersetzung fuer die vordefinierter Anweisung zum Erstellen eine neuen Vorlesung
                preSQL.setString(1, name);
                preSQL.setInt(2, roomnr);
                preSQL.setInt(3, professornr);
                preSQL.setInt(4, coursenr);
                preSQL.executeQuery();             // Ausfuehrung der vordef. Anweisung
                success = true;
                System.out.println("Der Datensatz wurde erfolgreich erstellt.\n");

            } catch(SQLException e) {
                success = false;
            }
            //----------------------------------------------------------------------------------------

            con.close();                                // Verbindung zur Datenbank trennen
            System.out.println("Die Verbindung zur Datenbank wurde getrennt!");
        } catch (SQLException e) {
            success = false;
        }
        //----------------------------------------------------------------------------------------
        return success;
    }


    /**
     * <p>Die Methode <b>getAllLectures</b> dient zum Ausgeben aller gespeicherten
     * Datensaetze in der Relation "Vorlesung".
     * </p>
     * @return  result      vom Typ <b>ResultSet</b> als Ergebnismenge
     */
    public ResultSet getAllLecture() {
        //----------------------------------------------------------------------------------------
        // Verbindungsversuch zur Datenbank
        try {
            con = getConnection();
            System.out.println("Verbindung zur Datenbank erfolgreich hergestellt.");

            //----------------------------------------------------------------------------------------
            // SQL Anweisung zum Abfragen aller gespeicherten Vorlesungen
            try {
                PreparedStatement preSQL = con.prepareStatement("SELECT * FROM Vorlesung");
                // Ueberpruefung ob Tabelle existiert
                if(preSQL.executeQuery() == null) {
                    throw new SQLException("Die Tabelle Vorlesung existiert nicht!");
                } else {
                    result = preSQL.executeQuery();
                }
            } catch (SQLException e) {
                result = null;
            }
            //----------------------------------------------------------------------------------------

            con.close();                                // Verbindung zur Datenbank trennen
            System.out.println("Die Verbindung zur Datenbank wurde getrennt!");
        } catch(SQLException e) {
            result = null;
        }
        //----------------------------------------------------------------------------------------
        return result;
    }


    /**
     * <p>Die Methode <b>getLectureById</b> dient zur Abfrage eines Datensatzes in der
     * Tabelle "Vorlesung" mittels der Angabe des Primaerschluessels.
     * </p>
     * @param id            vom Typ <b>int</b>
     * @return  result      vom Typ <b>ResultSet</b> als Ergebnismenge
     */
    public ResultSet getLectureById(int id) {
        return null;
    }


    /**
     * <p>Die Methode <b>editLecture</b> dient zur Aktualisierung eines Datensatzes
     * mittels der Angabe aller zu aendernden Attribute in der Tabelle "Vorlesung".
     * </p>
     * @param id            vom Typ <b>int</b>
     * @param name          vom Typ <b>String</b>
     * @param roomnr        vom Typ <b>int</b>
     * @param professornr   vom Typ <b>int</b>
     * @param coursenr      vom Typ <b>int</b>
     * @return success      vom Typ <b>boolean</b> wenn Aktualisierung erfolgreich <p>true</p>,
     *                      andernfalls <p>false</p>
     */
    public boolean editLecture(int id, String name, int roomnr, int professornr, int coursenr) {
        boolean success = false;
        //----------------------------------------------------------------------------------------
        // Verbindungsversuch zur Datenbank
        try {
            con = getConnection();
            System.out.println("Verbindung zur Datenbank erfolgreich hergestellt.");

            //----------------------------------------------------------------------------------------
            // SQL Anweisung zum Abfragen aller gespeicherten Vorlesungen
            try {
                PreparedStatement preSQL = con.prepareStatement("SELECT * FROM Vorlesung WHERE id=?");
                preSQL.setInt(1, id);
                // Ueberpreufung ob Tabelle Vorlesung existiert
                if (preSQL.executeQuery() == null) {
                    throw new SQLException("Die Tabelle Vorlesung existiert nicht!");
                } else {
                    PreparedStatement preSQL2 = con.prepareStatement("UPDATE Vorlesung " +
                            "SET (Name, raumNr, ProfessorenNr, VorlesungsNr)" +
                            "VALUES(?, ?, ?, ?)");
                    // Parametersetzung der 2. vordefinierten Anweisung
                    preSQL2.setString(1, name);
                    preSQL2.setInt(2, roomnr);
                    preSQL2.setInt(3, professornr);
                    preSQL2.setInt(4, coursenr);
                    preSQL2.executeQuery();
                }
            } catch (SQLException e) {
                success = false;
            }
            //----------------------------------------------------------------------------------------

            con.close();                                // Verbindung zur Datenbank trennen
            System.out.println("Die Verbindung zur Datenbank wurde getrennt!");
        } catch(SQLException e) {
            success = false;
        }
        //----------------------------------------------------------------------------------------
        return success;
    }


    public boolean deleteLectureById(int id) {
        return true;
    }


    //----------------------------------------------------------------------------------------
    // Implementierung zum Bearbeiten der Entity "PROFESSOR"
    //----------------------------------------------------------------------------------------

    public boolean createProfessor(String name, String firstname, String email, String tel) {
        boolean success = false;
        //----------------------------------------------------------------------------------------
        // Verbindungsversuch zur Datenbank
        try {
            con = getConnection();
            //----------------------------------------------------------------------------------------
            // SQL Anweisung zum erstellen einer neuen Vorlesung
            try {
                // Ueberpruefung ob Tabelle existiert
                if(!checkForTable("tbl_professor")) {
                    System.err.println("Die Tabelle Vorlesung exisitiert nicht. Es wird eine neue Tabelle angelegt...\n");
                    PreparedStatement preSQL2 = con.prepareStatement("CREATE TABLE tbl_professor " +
                            "(professorenNr INTEGER NOT NULL AUTO_INCREMENT, name VARCHAR(30) NOT NULL, " +
                            "vorname VARCHAR(30) NOT NULL, email VARCHAR(100) NOT NULL, telNr VARCHAR(30)," +
                            "PRIMARY KEY(professorenNr))");
                    System.out.println("Erstelle neue Tabelle Professor...\n");
                    preSQL2.execute();
                    System.out.println("Es wurde eine neue Tabelle Professor angelegt!");
                }
                PreparedStatement preSQL = con.prepareStatement("INSERT INTO tbl_professor " +
                        "(name, vorname, email, telNr)" +
                        "VALUES(?, ?, ?, ?)");
                // Parametersetzung fuer die vordefinierter Anweisung zum Erstellen eine neuen Vorlesung
                preSQL.setString(1, name);
                preSQL.setString(2, firstname);
                preSQL.setString(3, email);
                preSQL.setString(4, tel);

                System.out.println("Datensatz wird erstellt...\n");
                preSQL.execute();                           // Ausfuehrung der vordef. Anweisung

                con.close();                                // Verbindung zur Datenbank trennen
                success = true;
                System.out.println("Der Datensatz wurde erfolgreich erstellt.");
                System.out.println("-----------------------------------------------------------------\n");
            } catch(SQLException e) {
                success = false;
            }
            //----------------------------------------------------------------------------------------
            System.out.println("Die Verbindung zur Datenbank wurde getrennt!");
        } catch (SQLException e) {
            success = false;
        }
        //----------------------------------------------------------------------------------------
        return success;
    }


    public static void main(String[] args) throws SQLException {

        Connection con;
        DBMiddle test1 = new DBMiddle("codd.2clever4you.net/db204", "db204", "bleviker");
        test1.createRoom(55);

        ResultSet tmp = test1.getRoomById(1);
        while(tmp.next()) {
            System.out.println("| RaumNr: " + tmp.getString(1) + "  |  Sitzplätze: " + tmp.getString(2) + " |");
        }
        ResultSet tmp2 = test1.getAllRooms();
        while(tmp2.next()) {
            System.out.println("| RaumNr: " + tmp2.getString(1) + "   |  Sitzplätze: " + tmp2.getString(2));
        }

        test1.editRooom(5, 87);
        test1.deleteRoomById(5);

        test1.createProfessor("Klaus", "Sven", "s.klaus@hs-mannheim.de", "0621-00000");
    }

}
