package uk.gov.nationalarchives.confirmer

import uk.gov.nationalarchives.confirmer.Confirmer.Result

trait Confirmer:
  def getResult(payload: Payload, confirmationService: ConfirmationService): Result

object Confirmer:

  enum Result:
    case Success(dynamoMap: Map[String, List[String]])
    case Failure(errorMessage: Throwable)

    def isSuccess: Boolean = this match
      case _: Success => true
      case _: Failure => false

    def isError: Boolean = !isSuccess

  def getConfirmer(config: Config): Confirmer =
    ResultAttributeName.fromString(config.dynamoAttributeName) match
      case ResultAttributeName.RESULT_TC => tcConfirmer
      case ResultAttributeName.RESULT_CC => ccConfirmer

  val ccConfirmer: Confirmer =
    (payload: Payload, confirmationService: ConfirmationService) =>
      payload match
        case cc: CCPayload =>
          confirmationService match
            case ccService: CCService =>
              val objectFilePaths = ccService.ocfl.getFilePathsForObject(cc.preservationSystemId)
              if objectFilePaths.nonEmpty then Result.Success(Map("filePaths" -> objectFilePaths))
              else Result.Failure(new RuntimeException(s"filePaths could not be found for ${cc.preservationSystemId.toString}"))
            case _ => Result.Failure(new RuntimeException(s"Unsupported service in CC confirmer ${cc.preservationSystemId.toString}"))
        case _ => Result.Failure(new IllegalArgumentException("Invalid payload type for CC confirmer"))

  val tcConfirmer: Confirmer =
    (payload: Payload, confirmationService: ConfirmationService) =>
      payload match
        case tc: TCPayload =>
          confirmationService match
            case tcService: TCService =>
              val tapeConfirmation = tcService.scoutAM.getFileDetails(tc.filePaths)
              if tapeConfirmation.nonEmpty then Result.Success(tapeConfirmation)
              else Result.Failure(new RuntimeException(s"Volumes could not be found for one or more files in [${tc.filePaths.mkString(", ")}]"))
            case _ => Result.Failure(new RuntimeException("Unsupported service in TC confirmer"))
        case _ => Result.Failure(new IllegalArgumentException("Invalid payload type for TC confirmer"))
