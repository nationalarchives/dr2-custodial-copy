package uk.gov.nationalarchives.confirmer

import uk.gov.nationalarchives.confirmer.Confirmer.Result

import java.net.http.HttpClient

trait Confirmer:
  def getResult(payload: Payload): Result

object Confirmer:

  enum Result:
    case Success(dynamoMap: Map[String, List[String]])
    case Failure(errorMessage: Throwable)

    def isSuccess: Boolean = this match
      case _: Success => true
      case _: Failure => false

    def isError: Boolean = !isSuccess

  def getConfirmer(config: Config): Confirmer =
    config match
      case config: CCConfig => ccConfirmer(Ocfl(config))
      case config: TCConfig => tcConfirmer(ScoutAM(config, ScoutAmHttpService(HttpClient.newHttpClient())))

  val ccConfirmer: Ocfl => Confirmer =
    ocfl => {
      case cc: CCPayload =>
        val objectFilePaths = ocfl.getFilePathsForObject(cc.preservationSystemId)
        if objectFilePaths.nonEmpty then Result.Success(Map("filePaths" -> objectFilePaths))
        else Result.Failure(new RuntimeException(s"filePaths could not be found for ${cc.preservationSystemId.toString}"))
      case _ => Result.Failure(new IllegalArgumentException("Invalid payload type for CC confirmer"))
    }

  val tcConfirmer: ScoutAM => Confirmer =
    scoutAM => {
      case tc: TCPayload =>
        val tapeConfirmation = scoutAM.getFileDetails(tc.filePaths)
        if tapeConfirmation.nonEmpty then Result.Success(tapeConfirmation)
        else Result.Failure(new RuntimeException(s"Volumes could not be found for one or more files in [${tc.filePaths.mkString(", ")}]"))
      case _ => Result.Failure(new IllegalArgumentException("Invalid payload type for TC confirmer"))
    }
