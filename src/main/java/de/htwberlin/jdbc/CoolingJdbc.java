package de.htwberlin.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import de.htwberlin.exceptions.CoolingSystemException;
import org.dbunit.DatabaseUnitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.htwberlin.domain.Sample;
import de.htwberlin.exceptions.DataException;

public class CoolingJdbc implements ICoolingJdbc {

  private static final Logger L = LoggerFactory.getLogger(CoolingJdbc.class);
  private Connection connection;

  @Override
  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  @SuppressWarnings("unused")
  private Connection useConnection() {
    if (connection == null) {
      throw new DataException("Connection not set");
    }
    return connection;
  }

  @Override
  public List<String> getSampleKinds() {
    L.info("getSampleKinds: start");
    String sql = "SELECT TEXT FROM SAMPLEKIND";
    List<String> sampleKinds = new LinkedList<>();
    try(PreparedStatement ps = useConnection().prepareStatement(sql)){
      try(ResultSet rs = ps.executeQuery()){
        while(rs.next()){
          sampleKinds.add(rs.getString("TEXT"));
        }
      }catch( DataException e){
        L.info("", e);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    Collections.sort(sampleKinds);
    L.info(sampleKinds.toString());
    return sampleKinds;
  }

  @Override
  public Sample findSampleById(Integer sampleId) throws SQLException {
    L.info("findSampleById: sampleId: " + sampleId);
    String sql = String.join(" ",
            "SELECT *",
            "FROM SAMPLE",
            "WHERE SAMPLEID=?");
    Sample sample = null;
    try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {

      stmt.setInt(1, sampleId);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          int sampleID = rs.getInt("SAMPLEID");
          int sampleKindID = rs.getInt("SAMPLEKINDID");
          LocalDate date = rs.getDate("EXPIRATIONDATE").toLocalDate();
          sample = new Sample(sampleID, sampleKindID, date);
        } else {
          throw new CoolingSystemException("Es gibt die ID: " + sampleId + "nicht.");
        }
      }


    } catch(SQLException e) {
      L.info("Es gab folgenden Connection Fehler: " + e);
      throw new SQLException("Es gab einen Fehler bei der Connection: " + e);
    }

    return sample;
  }

  @Override
  public void createSample(Integer sampleId, Integer sampleKindId) throws SQLException {
    L.info("createSample: sampleId: " + sampleId + ", sampleKindId: " + sampleKindId);

    // Select DATE
    LocalDate date = LocalDate.now();
    L.info("Date wird berechnet");
    String sql_select = String.join(" ",
            "SELECT VALIDNOOFDAYS",
            "FROM SAMPLEKIND",
            "WHERE SAMPLEKINDID=?"
    );
    try(PreparedStatement stmt = useConnection().prepareStatement(sql_select)) {
      stmt.setInt(1, sampleKindId);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          int daysToAdd = rs.getInt("VALIDNOOFDAYS");
          date = date.plusDays(daysToAdd);
        } else {
          throw new CoolingSystemException("Es gab den SAMPLEKIND mit der SAMPLEKINDID: " + sampleKindId + "nicht.");
        }
      }

    } catch (SQLException e) {
      throw new SQLException("Es gab eine SQLException: " + e);
    }

    // Prüft, ob sampleID bereits existiert
    L.info("Check, ob die sampleID bereits existiert - START");
    String sql_checkSampleiD = String.join(" ",
            "SELECT *",
            "FROM SAMPLE",
            "WHERE SAMPLEID=?");
    try (PreparedStatement stmt = useConnection().prepareStatement(sql_checkSampleiD)) {
      stmt.setInt(1, sampleId);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          throw new CoolingSystemException("Sample mit der SAMPLEID: " + sampleId + " existiert bereits.");
        }
      }
    }
    L.info("Check, ob die SampleID existiert - ENDE");

    // Macht neuen Datensatz in die Datenbank
    L.info("Neue Datensatz wird in die Datenbank gepackt");
    String sql = String.join(" ",
            "INSERT INTO SAMPLE(SAMPLEID, SAMPLEKINDID, EXPIRATIONDATE)",
            "VALUES(?, ?, ?)"
    );
    try (PreparedStatement stmt = useConnection().prepareStatement(sql)) {
      stmt.setInt(1, sampleId);
      stmt.setInt(2, sampleKindId);
      stmt.setDate(3, java.sql.Date.valueOf(date));
      stmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException("Es gab einen Fehler: " + e);
    }

  }

  @Override
  public void clearTray(Integer trayId) {
    L.info("clearTray: trayId: " + trayId);


try{
  String selectTrayId = "SELECT * FROM TRAY WHERE TRAYID=?";
    PreparedStatement selectTrayIdStmt = useConnection().prepareStatement(selectTrayId);
    selectTrayIdStmt.setInt(1, trayId);
    if(!selectTrayIdStmt.executeQuery().next()){
      throw new CoolingSystemException("Tray mit der ID: " + trayId + " existiert nicht.");
    }
    try {
      // Schritt 1: Query, um die SampleIDs aus der Place-Tabelle zu selektieren
      String selectSampleIDsFromPlace = "SELECT DISTINCT SAMPLEID FROM PLACE WHERE TRAYID=?";
      PreparedStatement selectSampleIDsStmt = useConnection().prepareStatement(selectSampleIDsFromPlace);
      selectSampleIDsStmt.setInt(1, trayId);

      List<Integer> sampleIDs = new ArrayList<>();
      try (ResultSet rs = selectSampleIDsStmt.executeQuery()) {
        while (rs.next()) {
          sampleIDs.add(rs.getInt("SAMPLEID"));
        }
      }

      // Schritt 2: Lösche Datensätze aus der Place-Tabelle
      String deleteFromPlace = "DELETE FROM PLACE WHERE TRAYID=?";
      PreparedStatement deleteFromPlaceStmt = useConnection().prepareStatement(deleteFromPlace);
      deleteFromPlaceStmt.setInt(1, trayId);
      deleteFromPlaceStmt.executeUpdate();

      // Schritt 3: Lösche Datensätze aus der Samples-Tabelle
      for (Integer sampleId : sampleIDs) {
        String deleteFromSamples = "DELETE FROM SAMPLE WHERE SAMPLEID=?";
        PreparedStatement deleteFromSamplesStmt = useConnection().prepareStatement(deleteFromSamples);
        deleteFromSamplesStmt.setInt(1, sampleId);
        int rowsDeleted = deleteFromSamplesStmt.executeUpdate();

        if (rowsDeleted == 0) {
          L.warn("Sample with ID " + sampleId + " not found in SAMPLE table.");
        } else {
          L.info("Sample with ID " + sampleId + " deleted from SAMPLE table.");
        }
      }

      L.info("Tray with ID " + trayId + " cleared successfully.");

    } catch (SQLException e) {

    }
  }catch (SQLException e){

  }
  }

}


